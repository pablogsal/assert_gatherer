import json
import os
import tempfile
import ast
import asyncio
import re
import aiohttp
import warnings
import tarfile
import zipfile
import logging
from typing import List, Dict, Any
from rich.progress import Progress, TaskID
from rich.console import Console

FILE_LOCK = asyncio.Lock()

async def read_json_file(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as f:
        return json.load(f)

async def get_github_url(session: aiohttp.ClientSession, package_name: str) -> str:
    pypi_url = f"https://pypi.org/pypi/{package_name}/json"
    async with session.get(pypi_url) as response:
        if response.status == 200:
            data = await response.json()
            project_urls = data.get('info', {}).get('project_urls', {})

            # Try to find GitHub URL from project_urls by matching against a url regexp
            regexp = re.compile(r'(https?://github\.com/[^/]+/[^/]+)')
            for value in project_urls.values():
                match = regexp.match(value)
                if match:
                    return match.group(1)

            # Try to find GitHub URL from project_urls by checking for common keys
            for possible_key in ['Source', 'Code', 'Repository', "GitHub: repo", "Source Code", "Homepage", "GitHub"]:
                url = project_urls.get(possible_key) or project_urls.get(possible_key.lower())
                if url and 'github.com' in url:
                    return url
            for possible_issue_key in ['Issues', 'Bug Tracker', 'Bug Reports']:
                issue_url = project_urls.get(possible_issue_key) or project_urls.get(possible_issue_key.lower())
                if issue_url and 'github.com' in issue_url:
                    return issue_url.replace('/issues', '')
    return ''


async def get_sdist_url(session: aiohttp.ClientSession, package_name: str) -> str:
    pypi_url = f"https://pypi.org/pypi/{package_name}/json"
    async with session.get(pypi_url) as response:
        if response.status == 200:
            data = await response.json()
            releases = data.get('releases', {})
            latest_version = data['info']['version']
            for url_info in releases.get(latest_version, []):
                if url_info['packagetype'] == 'sdist':
                    return url_info['url']
    return ''

async def download_and_extract_sdist(session: aiohttp.ClientSession, sdist_url: str, temp_dir: str) -> str:
    async with session.get(sdist_url) as response:
        if response.status == 200:
            content = await response.read()
            file_name = os.path.basename(sdist_url)
            file_path = os.path.join(temp_dir, file_name)
            with open(file_path, 'wb') as f:
                f.write(content)
            
            extracted_dir = os.path.join(temp_dir, 'extracted')
            os.makedirs(extracted_dir, exist_ok=True)
            
            if file_name.endswith('.tar.gz') or file_name.endswith('.tgz'):
                with tarfile.open(file_path, 'r:gz') as tar:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        tar.extractall(path=extracted_dir)
            elif file_name.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_dir)
            else:
                raise ValueError(f"Unsupported file format: {file_name}")
            
            return extracted_dir
    raise ValueError(f"Failed to download sdist from {sdist_url}")

async def clone_repo(github_url: str, temp_dir: str) -> str:
    repo_dir = os.path.join(temp_dir, github_url.split('/')[-1])
    process = await asyncio.create_subprocess_exec(
        'git', 'clone', '--depth', '1', github_url, repo_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await process.communicate()
    return repo_dir

class AssertFinder(ast.NodeVisitor):
    def __init__(self):
        self.asserts = []

    def visit_Assert(self, node):
        self.asserts.append(ast.unparse(node).strip())

def count_py_files(repo_dir: str) -> int:
    return sum(1 for root, _, files in os.walk(repo_dir) for file in files if file.endswith('.py'))

async def find_asserts(repo_dir: str, progress: Progress, task_id: TaskID) -> List[str]:
    assert_finder = AssertFinder()
    for root, _, files in os.walk(repo_dir):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            tree = ast.parse(f.read())
                            assert_finder.visit(tree)
                except SyntaxError:
                    print(f"Syntax error in file: {file_path}")
                progress.update(task_id, advance=1)
                await asyncio.sleep(0)
    return assert_finder.asserts

async def process_package(session: aiohttp.ClientSession, package_name: str, temp_dir: str, progress: Progress, overall_task: TaskID, semaphore: asyncio.Semaphore,
                          results_file) -> None:
    async with semaphore:
        try:
            github_url = await get_github_url(session, package_name)
        except Exception as e:
            print(f"Error getting GitHub URL for {package_name}: {str(e)}")
            progress.update(overall_task, advance=1)
            return
        if not github_url:
            print(f"No GitHub URL found for {package_name}, trying sdist...")
            sdist_url = await get_sdist_url(session, package_name)
            if not sdist_url:
                print(f"No sdist URL found for {package_name}")
                progress.update(overall_task, advance=1)
                return 
            try:
                repo_dir = await download_and_extract_sdist(session, sdist_url, temp_dir)
            except Exception as e:
                print(f"Error downloading or extracting sdist for {package_name}: {str(e)}")
                progress.update(overall_task, advance=1)
                return 
        else:
            try:
                repo_dir = await clone_repo(github_url, temp_dir)
            except Exception as e:
                print(f"Error cloning repo for {package_name}: {str(e)}")
                progress.update(overall_task, advance=1)
                return 

        package_task = None
        try:
            repo_dir = await clone_repo(github_url, temp_dir)
            py_file_count = count_py_files(repo_dir)
            package_task = progress.add_task(f"[cyan]Processing {package_name}", total=py_file_count)
            asserts = await find_asserts(repo_dir, progress, package_task)
            progress.update(overall_task, advance=1)
            progress.remove_task(package_task)
            result = {package_name: asserts}
            async with FILE_LOCK:
                results_file.write(json.dumps(result) + '\n')
                results_file.flush()
            return
        except Exception as e:
            print(f"Error processing {package_name}: {str(e)}")
            progress.update(overall_task, advance=1)
            if package_task:
                progress.remove_task(package_task)
            return

async def main():
    json_file = 'top-pypi-packages/top-pypi-packages-30-days.json'
    data = await read_json_file(json_file)
    
    max_concurrent = 100
    semaphore = asyncio.Semaphore(max_concurrent)

    console = Console()
    all_packages = data['rows']
    with Progress(console=console) as progress:
        overall_task = progress.add_task("[green]Processing packages", total=len(all_packages))
        

        with (
            tempfile.TemporaryDirectory() as temp_dir,
            open('results.json', 'w') as results_file,
        ):
            async with (
                aiohttp.ClientSession() as session,
                asyncio.TaskGroup() as tg,
            ):
                for package in all_packages:
                    tg.create_task(process_package(session, package['project'], temp_dir, progress, overall_task, semaphore, results_file))

if __name__ == "__main__":
    asyncio.run(main())