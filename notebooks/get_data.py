import asyncio
import logging
import pathlib

import httpx

BASE_URL = "http://data.insideairbnb.com"
URLS = {"/denmark/hovedstaden/copenhagen": ["2023-03-31", "2022-12-29", "2022-09-24"]}

logging.basicConfig(
    datefmt="%Y-%m-%d %H:%M:%S", format="[%(asctime)s] %(message)s", level=logging.INFO
)


async def write_file(queue: asyncio.Queue):
    while True:
        data = await queue.get()
        filename = (
            pathlib.Path.cwd()
            .joinpath(data["url"][1:].replace("/", "_"))
            .with_suffix(".csv")
        )
        logging.info(f"Writing {filename}")
        filename.write_text(data["content"])

        queue.task_done()


async def download_file(
    client: httpx.AsyncClient, in_queue: asyncio.Queue, out_queue: asyncio.Queue
):
    while True:
        url = await in_queue.get()
        r = await client.get(url)
        logging.info(f"Downloaded {url}")
        await out_queue.put({"url": url, "content": r.text})
        in_queue.task_done()


async def main():
    logging.info("Starting download")
    urls_to_process = [
        f"{city}/{date}/visualisations/listings.csv"
        for city in URLS
        for date in URLS[city]
    ]

    url_queue = asyncio.Queue()

    for url in urls_to_process:
        await url_queue.put(url)

    content_queue = asyncio.Queue()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        download_tasks = [
            asyncio.create_task(download_file(client, url_queue, content_queue))
            for _ in range(3)
        ]
        await url_queue.join()
        for task in download_tasks:
            task.cancel()
        await asyncio.gather(*download_tasks, return_exceptions=True)

    write_tasks = [asyncio.create_task(write_file(content_queue)) for _ in range(3)]

    await content_queue.join()

    for task in write_tasks:
        task.cancel()
    await asyncio.gather(*write_tasks, return_exceptions=True)
    logging.info("Completed download")


if __name__ == "__main__":
    asyncio.run(main())
