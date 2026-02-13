from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task

from zepto_scraper import scrape_product


def chunked(items: list[dict[str, Any]], size: int = 1000):
    for index in range(0, len(items), size):
        yield items[index : index + size]


@task(retries=3, retry_delay_seconds=10)
def scrape_task(pdp_url: str, pincode: str) -> dict[str, Any]:
    result = scrape_product(pdp_url, pincode)
    if result is None:
        return {"status": "failed", "url": pdp_url, "pincode": pincode}

    if hasattr(result, "to_dict"):
        payload = result.to_dict()
    else:
        payload = dict(result)

    payload["status"] = "success"
    payload["url"] = pdp_url
    payload["pincode"] = pincode
    return payload


@flow(name="zepto-batch")
def run_batch(products: list[dict[str, str]], chunk_size: int = 1000) -> dict[str, int]:
    logger = get_run_logger()
    total = len(products)
    success = 0
    failed = 0

    if total == 0:
        return {"total": 0, "success": 0, "failed": 0}

    for batch_number, group in enumerate(chunked(products, chunk_size), start=1):
        logger.info("Submitting chunk %s with %s products", batch_number, len(group))
        futures = scrape_task.map(
            [item["url"] for item in group],
            [item["pincode"] for item in group],
        )

        for future in futures:
            try:
                data = future.result()
                if isinstance(data, dict) and data.get("status") == "success":
                    success += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        logger.info(
            "Chunk %s completed. Running totals -> success: %s, failed: %s",
            batch_number,
            success,
            failed,
        )

    summary = {"total": total, "success": success, "failed": failed}
    logger.info("Batch complete: %s", summary)
    return summary


if __name__ == "__main__":
    sample_products = [
        {
            "url": "https://www.zepto.com/pn/lizol-rose-fresh-shakti-disinfectant-floor-cleaner/pvid/9a7cdd91-3d2f-4b30-9219-9c66fd6950d8",
            "pincode": "500001",
        }
    ]
    print(run_batch(sample_products, chunk_size=1))
