# CdcWalV6: multi-stage build .NET 8 (pg_logical_emit_message + pgoutput messages=true + SuperFrame).
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY CdcWalV6/CdcWalV6.csproj CdcWalV6/
COPY CdcCommon/CdcCommon.csproj CdcCommon/
COPY CdcKafka/CdcKafka.csproj CdcKafka/
RUN dotnet restore CdcWalV6/CdcWalV6.csproj

COPY CdcWalV6/ CdcWalV6/
COPY CdcCommon/ CdcCommon/
COPY CdcKafka/ CdcKafka/
RUN dotnet publish CdcWalV6/CdcWalV6.csproj -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER app
ENTRYPOINT ["dotnet", "CdcWalV6.dll"]
