"""
LinkedIn MCP Server entry point.

Starts the MCP server with the configured transport.
FastMCP handles lifespan management automatically via the lifespan parameter in server.py.
"""

import os
import sys

from linkedin_mcp.server import mcp


def main() -> None:
    """Main entry point for the LinkedIn MCP Server.

    FastMCP handles all lifecycle management including:
    - Lifespan context (initialization/shutdown)
    - Transport selection (stdio by default)
    - Signal handling
    """
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    # FastMCP uses "sse" for its HTTP transport
    if transport == "streamable-http":
        transport = "sse"
        
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("MCP_PORT", "8000"))

    try:
        if transport == "stdio":
            mcp.run(transport="stdio")
        else:
            mcp.run(transport=transport, host=host, port=port)
    except KeyboardInterrupt:
        print("\nServer stopped by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
