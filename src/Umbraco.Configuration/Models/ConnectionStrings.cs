using System;
using System.Data.Common;
using Microsoft.Extensions.Configuration;
using Umbraco.Core;
using Umbraco.Core.Configuration;

namespace Umbraco.Configuration.Models
{
    public class ConnectionStrings : IConnectionStrings
    {
        private readonly IConfiguration _configuration;

        public ConnectionStrings(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public ConfigConnectionString this[string key]
        {
            get
            {
                string provider;
                var connectionString = _configuration.GetConnectionString(key);
                if (string.IsNullOrEmpty(connectionString))
                {
                    var connSection = _configuration.GetSection("ConnectionStrings")?.GetSection(key);
                    if (connSection != null)
                    {
                        connectionString = connSection.GetValue<string>("ConnectionString");
                        provider = connSection.GetValue<string>("ProviderName");
                        return new ConfigConnectionString(connectionString, provider, key);
                    }
                }
                provider = ParseProvider(connectionString);
                return new ConfigConnectionString(connectionString, provider, key);
            }
            set => throw new NotImplementedException();
        }

        private string ParseProvider(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return null;
            }

            var builder = new DbConnectionStringBuilder();

            builder.ConnectionString = connectionString;

            if (builder.TryGetValue("Data Source", out var ds) && ds is string dataSource)
            {
                if (dataSource.EndsWith(".sdf"))
                {
                    return Constants.DbProviderNames.SqlCe;
                }
            }

            if (builder.TryGetValue("Server", out var s) && s is string server && !string.IsNullOrEmpty(server))
            {
                if (builder.TryGetValue("Database", out var db) && db is string database && !string.IsNullOrEmpty(database))
                {
                    return Constants.DbProviderNames.SqlServer;
                }

                if (builder.TryGetValue("AttachDbFileName", out var a) && a is string attachDbFileName && !string.IsNullOrEmpty(attachDbFileName))
                {
                    return Constants.DbProviderNames.SqlServer;
                }

                if (builder.TryGetValue("Initial Catalog", out var i) && i is string initialCatalog && !string.IsNullOrEmpty(initialCatalog))
                {
                    return Constants.DbProviderNames.SqlServer;
                }
            }

            throw new ArgumentException("Cannot determine provider name from connection string", nameof(connectionString));
        }
    }
}
