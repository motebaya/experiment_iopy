#!/usr/bin/python3
# @github.com/motebaya - 19.01.2024
# experiment_io.py

import aiofiles, asyncio, time, requests, io, os, inspect
from rich.console import Console
from concurrent.futures import ThreadPoolExecutor
from aiohttp import ClientSession
from typing import List
from rich.progress import (
    Progress, 
    SpinnerColumn, 
    BarColumn, 
    TextColumn, 
    DownloadColumn, 
    TransferSpeedColumn, 
    TimeRemainingColumn
)

class Coros:
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
    @staticmethod
    async def asyncdownload(url: str, fname: str) -> str:
        """
        -> async download
        """
        async with ClientSession(headers=Coros.headers) as session:
            async with session.get(url) as response:
                async with aiofiles.open(fname, "wb") as f:
                    with Progress(
                        SpinnerColumn(speed=1.5),
                        TextColumn("[green] Downloading..", justify="right"),
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.0f}%",
                        DownloadColumn(binary_units=False),
                        TransferSpeedColumn(),
                        TimeRemainingColumn(),
                        console=Console(),
                        transient=True
                    ) as progress:
                        task = progress.add_task("[green] Downloading..", total=int(response.headers.get('content-length', 0)))
                        async for content in response.content.iter_any():
                            await f.write(content)
                            progress.update(
                                task, advance=len(content)
                            )
                        await f.close()
                        progress.stop()
                Console().print(f"[yellow] Asyncdownload[reset] Completed -> [yellow]{fname}[reset]")
                return fname

    @staticmethod
    def syncdownload(url: str, fname: str) -> str:
        """
        -> sync download
        """
        response = requests.get(url, headers=Coros.headers, stream=True)
        with io.open(fname, "wb") as f:
            with Progress(SpinnerColumn(speed=1.5),
                TextColumn("[green] Downloading..", justify="right"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                DownloadColumn(binary_units=False),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=Console(),
                transient=True
            ) as progress:
                task = progress.add_task("[green] Downloading..", total=int(response.headers.get('content-length', 0)))
                for content in response.iter_content(1024):
                    f.write(content)
                    progress.update(task, advance=len(content))
                f.close()
                progress.stop()
        Console().print(f"[green] Syncdownload[reset] Completed -> [green]{fname}[reset]")
        return fname

    @staticmethod
    def syncopen(file: str) -> str | bytes:
        """
        -> sync open
        """
        with io.open(file, "rb") as f:
            content = f.read()
        length = len(content)
        Console().print(f"[green] syncopen[reset] ok open length -> [green]{length}[reset]")
        return str(length)
    
    @staticmethod
    async def asyncopen(file: str) -> str | bytes:
        """
        -> async open
        """
        async with aiofiles.open(file, mode="rb") as f:
            content = await f.read()
        length = len(content)
        Console().print(f"[yellow] asyncopen[reset] ok open length -> [yellow]{length}[reset]")
        return str(length)
    
    @staticmethod
    def block_io_open(files: List[str]) -> List[str]:
        """
        -> test
        -> read bulk files in concurrency with separated thread, 
        but each individual thread still blocked and waiting till reading operation completed.
        """
        with ThreadPoolExecutor(max_workers=15) as thread:
            results = list(thread.map(
                lambda file: Coros.syncopen(
                    file
                ), files
            ))
        return results
    
    @staticmethod
    async def non_block_io_open(files: List[str]) -> List[str]:
        """
        -> test
        -> read bulk files in single thread, 
        and no need to wait reading operation till complete before next.
        """
        results = await asyncio.gather(*[
            Coros.asyncopen(
                file
            ) for file in files
        ])
        return results
    
    @staticmethod
    async def non_block_io_download(urls) -> None:
        """
        -> test
        -> async call with non IO block, it's mean no need wait till process
        completed to move others task. it can be run in parallel/concurrency.
        """
        results = await asyncio.gather(*[
            Coros.asyncdownload(
                url, url.split('/')[-1]
            ) for url in urls
        ])
        return results
    
    @staticmethod
    def block_io_download(urls) -> List[str]:
        """
        -> test
        -> call io block function in parallel/concurrency with separated threads.
        """
        with ThreadPoolExecutor(max_workers=15) as thread:
            results = list(thread.map(
                lambda url: Coros.syncdownload(
                    url, url.split("/")[-1]
                ), urls
            ))
        return results
    
    @staticmethod
    def primitif_io_block_download(urls):
        """
        -> test
        -> lazy call for io block, there's no concurrency and always be waiting process till completed before next.
        """
        results = [
            Coros.syncdownload(url, url.split("/")[-1]) for \
                url in urls
        ]
        return results

if __name__=="__main__":
    urls = [
        # "https://images8.alphacoders.com/134/1347725.png",
        # "https://images3.alphacoders.com/134/1347726.png",
        # "https://images8.alphacoders.com/134/1347727.png",
        # "https://speedtesta.kpn.com/10MB.bin",
        # "https://speedtesta.kpn.com/10MB.bin",
        # "https://speedtesta.kpn.com/10MB.bin",
        # "https://speedtesta.kpn.com/1MiB.bin",
        # "https://speedtesta.kpn.com/1MiB.bin",
        # "https://speedtesta.kpn.com/1MiB.bin",
        "https://singapore.downloadtestfile.com/5MB.bin",
        "https://singapore.downloadtestfile.com/5MB.bin",
        "https://singapore.downloadtestfile.com/5MB.bin",
    ]
    files = [
        i for i in os.listdir(
            os.path.dirname(__file__))\
                if not i.endswith('.py')
    ]
    import inspect
    for name, func in inspect.getmembers(Coros, lambda func: inspect.isfunction(func)):
        method = getattr(Coros, name)
        if "test" in method.__doc__:
            Console().print(f" *[red] Running: {name}[reset]")
            args = urls if name.lower().endswith(
                "_download"
            ) else files
            start = time.time()
            if inspect.iscoroutinefunction(method):
                # deprecated in python3.12+
                # loop = asyncio.get_event_loop() 
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(method(args))
                loop.close()
            else:
                method(args)
            print(f" --> Completed in {time.time() - start} seconds!", end="\n"+("-"*35)+"\n\n")