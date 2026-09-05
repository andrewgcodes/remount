"""Exercise the Python package against the real strict-profile Go relay/control."""

import asyncio
import sys

from remount import Client


async def main():
    url, profile = sys.argv[1:]
    async with Client(url, "sdk-synthetic-token") as client:
        assert client._controller_epoch > 0
        await asyncio.gather(*(client.call("ws.list") for _ in range(8)))
        workspace = await client.create_workspace({"name": "python-protocol-test"})
        try:
            assert workspace["spec"]["security"]["profile"] == profile
            assert (await client.get_workspace(workspace["id"]))["id"] == workspace["id"]
        finally:
            await client.destroy_workspace(workspace["id"])
    print(f"Python strict-profile protocol passed: {profile}")


asyncio.run(main())
