# Build stage for React SPA
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend

# Copy frontend files
COPY src/SDRLoggerPlus.Web/package*.json ./
RUN npm ci

COPY src/SDRLoggerPlus.Web/ ./
RUN npm run build

# Build stage for .NET
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS backend-build
WORKDIR /app

# Copy csproj files and restore
COPY SDRLoggerPlus.sln ./
COPY src/SDRLoggerPlus.Server/SDRLoggerPlus.Server.csproj ./src/SDRLoggerPlus.Server/
COPY src/SDRLoggerPlus.Contracts/SDRLoggerPlus.Contracts.csproj ./src/SDRLoggerPlus.Contracts/
RUN dotnet restore

# Copy source and build
COPY src/ ./src/
RUN dotnet publish src/SDRLoggerPlus.Server/SDRLoggerPlus.Server.csproj -c Release -o /app/publish

# Copy frontend build to wwwroot
COPY --from=frontend-build /app/frontend/dist /app/publish/wwwroot

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine AS runtime
WORKDIR /app

# Install icu-libs for globalization support
RUN apk add --no-cache icu-libs

# Set environment
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV ASPNETCORE_URLS=http://+:5050
ENV ASPNETCORE_ENVIRONMENT=Production

# Copy published app
COPY --from=backend-build /app/publish .

# Create non-root user and config directory
RUN addgroup -S sdrloggerplus && adduser -S sdrloggerplus -G sdrloggerplus \
    && mkdir -p /home/sdrloggerplus/.config/SDRLoggerPlus \
    && chown -R sdrloggerplus:sdrloggerplus /home/sdrloggerplus
USER sdrloggerplus

EXPOSE 5050

ENTRYPOINT ["dotnet", "SDRLoggerPlus.Server.dll"]
