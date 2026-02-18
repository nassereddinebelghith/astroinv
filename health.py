from __future__ import annotations

import asyncio
import aiohttp

from .models import AirflowHealthError, AirflowHealthResult, Instance


async def get_health(logger, session: aiohttp.ClientSession, inst: Instance) -> AirflowHealthResult:
    """Run one health check using a shared aiohttp session."""

    def get_major_version(version_str: str) -> int:
        """Safely extract major version from a version string.

        Returns 0 if the version isn't parseable (e.g., 'main').
        """
        try:
            if not version_str or not version_str[0].isdigit():
                logger.warning("Invalid version format: '%s'. Expected 'X.Y.Z'.", version_str)
                return 0
            return int(version_str.split(".")[0])
        except (ValueError, IndexError, AttributeError) as e:
            logger.warning("Failed to parse version '%s': %s", version_str, str(e))
            return 0

    major_version = get_major_version(inst.version)

    url = f"{inst.url}/health"
    if major_version >= 4:
        url = f"{inst.url}/api/v2/monitor/health"

    logger.debug("Healthcheck GET %s", url)

    try:
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return AirflowHealthResult(instance=inst, success=await resp.json())

            logger.error("Healthcheck failed: url=%s status=%s", url, resp.status)
            return AirflowHealthResult(
                instance=inst,
                failure=AirflowHealthError(message="HTTP error", status_code=resp.status),
            )
    except asyncio.TimeoutError:
        logger.error("Healthcheck timeout: url=%s", url)
        return AirflowHealthResult(
            instance=inst,
            failure=AirflowHealthError(message="Timeout", status_code=408),
        )
    except Exception as err:
        logger.error("Healthcheck exception: url=%s err=%s", url, err)
        return AirflowHealthResult(
            instance=inst,
            failure=AirflowHealthError(message=str(err), status_code=500),
        )
