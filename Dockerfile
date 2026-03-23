# LinkedIn MCP Server Dockerfile
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

# Limit exposed tools to RapidAPI only
RUN python -c "import re; \
keep = {'debug_context', 'get_profile', 'get_profile_interests', 'get_similar_profiles', 'get_profile_articles', 'get_article', 'get_company_by_domain', 'get_company', 'get_company_updates', 'search_people', 'search_companies', 'get_profile_posts', 'get_post_reactions', 'get_post_comments', 'analyze_engagement', 'analyze_content_performance', 'analyze_optimal_posting_times', 'analyze_post_audience', 'analyze_hashtag_performance', 'generate_engagement_report', 'get_auth_status', 'get_cache_stats'}; \
txt = open('src/linkedin_mcp/server.py').read(); \
txt = re.sub(r'@mcp\.tool\(\)\nasync def ([a-zA-Z0-9_]+)', lambda m: m.group(0) if m.group(1) in keep else f'async def {m.group(1)}', txt); \
open('src/linkedin_mcp/server.py', 'w').write(txt)"

# Install dependencies
RUN uv sync --frozen --no-dev

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV LOG_FORMAT=json

# --- RAILWAY WEB SERVER SETTINGS ---
ENV MCP_TRANSPORT=streamable-http
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
EXPOSE 8000

# Create volume links and start the server
CMD mkdir -p /data/app_data /data/cookies && \
    rm -rf /app/data /app/cookies && \
    ln -s /data/app_data /app/data && \
    ln -s /data/cookies /app/cookies && \
    uv run linkedin-mcp
