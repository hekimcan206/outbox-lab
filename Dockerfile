# syntax=docker/dockerfile:1

# ---- build -------------------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

# Restore in its own layer: it only re-runs when package versions or project files change.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/OutboxLab.Core/OutboxLab.Core.csproj src/OutboxLab.Core/
COPY src/OutboxLab.Postgres/OutboxLab.Postgres.csproj src/OutboxLab.Postgres/
COPY src/OutboxLab.Oracle/OutboxLab.Oracle.csproj src/OutboxLab.Oracle/
COPY src/OutboxLab.Runner/OutboxLab.Runner.csproj src/OutboxLab.Runner/
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet restore src/OutboxLab.Runner/OutboxLab.Runner.csproj

COPY .editorconfig ./
COPY src/ src/
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish src/OutboxLab.Runner/OutboxLab.Runner.csproj \
        --configuration Release --no-restore --output /app/publish -p:UseAppHost=false

# ---- runtime -----------------------------------------------------------------------------------------
# Chiseled Ubuntu: no shell, no package manager, non-root "app" user (UID 1654). The ASP.NET variant is used only
# because it shares layers with the other services of this portfolio; the lab itself needs just the .NET runtime.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime
LABEL org.opencontainers.image.title="outbox-lab" \
      org.opencontainers.image.source="https://github.com/hekimcan206/outbox-lab" \
      org.opencontainers.image.licenses="MIT"
WORKDIR /app
COPY --from=build --link /app/publish .
USER $APP_UID
ENTRYPOINT ["dotnet", "outbox-lab.dll"]
CMD ["demo"]
