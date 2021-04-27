using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NPoco;
using Umbraco.Core.Persistence.DatabaseAnnotations;
using Umbraco.Core.Persistence.DatabaseModelDefinitions;
using Umbraco.Core.Persistence.Querying;

namespace Umbraco.Core.Persistence.SqlSyntax
{
    /// <summary>
    /// Represents an SqlSyntaxProvider for Sql Server.
    /// </summary>
    public class PostgreSqlSyntaxProvider : SqlSyntaxProviderBase<PostgreSqlSyntaxProvider>, IMainDomLockSupportingSqlSyntaxProvider
    {
        public PostgreSqlSyntaxProvider()
        {
            StringColumnDefinition = "text";
            StringLengthColumnDefinitionFormat = "text";
            StringLengthUnicodeColumnDefinitionFormat = "text";
            StringLengthNonUnicodeColumnDefinitionFormat = "text";
            GuidColumnDefinition = "uuid";
            DateTimeColumnDefinition = "timestamp without time zone";
            TimeColumnDefinition = "time without time zone";
            InitColumnTypeMap();
        }

        protected void InitColumnTypeMap()
        {
            DbTypeMap.Set<string>(DbType.String, StringColumnDefinition);
            DbTypeMap.Set<char>(DbType.StringFixedLength, StringColumnDefinition);
            DbTypeMap.Set<char?>(DbType.StringFixedLength, StringColumnDefinition);
            DbTypeMap.Set<char[]>(DbType.String, StringColumnDefinition);
            DbTypeMap.Set<bool>(DbType.Boolean, BoolColumnDefinition);
            DbTypeMap.Set<bool?>(DbType.Boolean, BoolColumnDefinition);
            DbTypeMap.Set<Guid>(DbType.Guid, GuidColumnDefinition);
            DbTypeMap.Set<Guid?>(DbType.Guid, GuidColumnDefinition);
            DbTypeMap.Set<DateTime>(DbType.DateTime, DateTimeColumnDefinition);
            DbTypeMap.Set<DateTime?>(DbType.DateTime, DateTimeColumnDefinition);
            DbTypeMap.Set<TimeSpan>(DbType.Time, TimeColumnDefinition);
            DbTypeMap.Set<TimeSpan?>(DbType.Time, TimeColumnDefinition);
            DbTypeMap.Set<DateTimeOffset>(DbType.Time, TimeColumnDefinition);
            DbTypeMap.Set<DateTimeOffset?>(DbType.Time, TimeColumnDefinition);
        }

        public override string ProviderName => Constants.DbProviderNames.PostgreSql;

        public override string DbProvider { get; } = "PostgreSql";

        public override IEnumerable<string> GetTablesInSchema(IDatabase db)
        {
            var items = db.Fetch<dynamic>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = (SELECT current_schema())");
            return items.Select(x => x.TABLE_NAME).Cast<string>().ToList();
        }

        public override IsolationLevel DefaultIsolationLevel => IsolationLevel.ReadCommitted;

        public override IEnumerable<ColumnInfo> GetColumnsInSchema(IDatabase db)
        {
            var items = db.Fetch<dynamic>("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = (SELECT current_schema())");
            return
                items.Select(
                    item =>
                    new ColumnInfo(item.TABLE_NAME, item.COLUMN_NAME, item.ORDINAL_POSITION, item.COLUMN_DEFAULT,
                                   item.IS_NULLABLE, item.DATA_TYPE)).ToList();
        }

        /// <inheritdoc />
        public override IEnumerable<Tuple<string, string>> GetConstraintsPerTable(IDatabase db)
        {
            var items =
                db.Fetch<dynamic>(
                    "SELECT TABLE_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE WHERE TABLE_SCHEMA = (SELECT current_schema())");
            return items.Select(item => new Tuple<string, string>(item.TABLE_NAME, item.CONSTRAINT_NAME)).ToList();
        }

        /// <inheritdoc />
        public override IEnumerable<Tuple<string, string, string>> GetConstraintsPerColumn(IDatabase db)
        {
            var items =
                db.Fetch<dynamic>(
                    "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_SCHEMA = (SELECT current_schema())");
            return items.Select(item => new Tuple<string, string, string>(item.TABLE_NAME, item.COLUMN_NAME, item.CONSTRAINT_NAME)).ToList();
        }

        /// <inheritdoc />
        public override IEnumerable<Tuple<string, string, string, bool>> GetDefinedIndexes(IDatabase db)
        {
            var items =
                db.Fetch<dynamic>(@"
select
    ixs.tablename,
    ixs.indexname,
    a.attname as columnname,
	ix.indisunique as unique
from
    pg_class t,
    pg_class i,
    pg_index ix,
	pg_indexes ixs,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
	and ixs.indexname = i.relname
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and ixs.schemaname = (select current_schema())
order by
    t.relname,
    i.relname;");
            return items.Select(item => new Tuple<string, string, string, bool>(item.tablename, item.indexname, item.columnname,
                item.unique == true)).ToList();

        }

        /// <inheritdoc />
        public override bool TryGetDefaultConstraint(IDatabase db, string tableName, string columnName, out string constraintName)
        {
            constraintName = db.Fetch<string>(@"select con.[name] as [constraintName]
from sys.default_constraints con
join sys.columns col on con.object_id=col.default_object_id
join sys.tables tbl on col.object_id=tbl.object_id
where tbl.[name]=@0 and col.[name]=@1;", tableName, columnName)
                       .FirstOrDefault();
            return !constraintName.IsNullOrWhiteSpace();
        }

        public override bool DoesTableExist(IDatabase db, string tableName)
        {
            var result =
                db.ExecuteScalar<long>("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName AND TABLE_SCHEMA = (SELECT current_schema())",
                                       new { TableName = tableName });

            return result > 0;
        }

        public override void WriteLock(IDatabase db, params int[] lockIds)
        {
            WriteLock(db, TimeSpan.FromMilliseconds(1800), lockIds);
        }

        public void WriteLock(IDatabase db, TimeSpan timeout, params int[] lockIds)
        {
            // soon as we get Database, a transaction is started

            if (db.Transaction.IsolationLevel < IsolationLevel.ReadCommitted)
                throw new InvalidOperationException("A transaction with minimum ReadCommitted isolation level is required.");


            // *not* using a unique 'WHERE IN' query here because the *order* of lockIds is important to avoid deadlocks
            foreach (var lockId in lockIds)
            {
                db.Execute($"SET lock_timeout = {timeout.TotalMilliseconds};");
                var i = db.Execute(@"UPDATE ""umbracoLock"" SET value = (CASE WHEN (value=1) THEN -1 ELSE 1 END) WHERE id=@id", new { id = lockId });
                if (i == 0) // ensure we are actually locking!
                    throw new ArgumentException($"LockObject with id={lockId} does not exist.");
            }
        }


        public override void ReadLock(IDatabase db, params int[] lockIds)
        {
            // soon as we get Database, a transaction is started

            if (db.Transaction.IsolationLevel < IsolationLevel.ReadCommitted)
                throw new InvalidOperationException("A transaction with minimum ReadCommitted isolation level is required.");

            // *not* using a unique 'WHERE IN' query here because the *order* of lockIds is important to avoid deadlocks
            foreach (var lockId in lockIds)
            {
                var i = db.ExecuteScalar<int?>(@"SELECT value FROM ""umbracoLock"" WHERE id=@id", new { id = lockId });
                if (i == null) // ensure we are actually locking!
                    throw new ArgumentException($"LockObject with id={lockId} does not exist.", nameof(lockIds));
            }
        }

        public override string FormatColumnRename(string tableName, string oldName, string newName)
        {
            return string.Format(RenameColumn, tableName, oldName, newName);
        }

        public override string FormatTableRename(string oldName, string newName)
        {
            return string.Format(RenameTable, oldName, newName);
        }

        protected override string FormatIdentity(ColumnDefinition column)
        {
            return column.IsIdentity ? GetIdentityString(column) : string.Empty;
        }

        public override Sql<ISqlContext> SelectTop(Sql<ISqlContext> sql, int top)
        {
            return new Sql<ISqlContext>(sql.SqlContext, sql.SQL.Insert(sql.SQL.IndexOf(' '), " TOP " + top), sql.Arguments);
        }

        private static string GetIdentityString(ColumnDefinition column)
        {
            return "GENERATED BY DEFAULT AS IDENTITY";
        }

        protected override string FormatSystemMethods(SystemMethods systemMethod)
        {
            switch (systemMethod)
            {
                case SystemMethods.NewGuid:
                    return "uuid_generate_v4()";
                case SystemMethods.CurrentDateTime:
                    return "NOW()";
                //case SystemMethods.NewSequentialId:
                //    return "NEWSEQUENTIALID()";
                //case SystemMethods.CurrentUTCDateTime:
                //    return "GETUTCDATE()";
            }

            return null;
        }

        public override string GetStringColumnEqualComparison(string column, int paramIndex, TextColumnType columnType)
        {
            switch (columnType)
            {
                case TextColumnType.NVarchar:
                case TextColumnType.NText:
                    return $"\"{column}\" LIKE @{paramIndex}";
                default:
                    throw new ArgumentOutOfRangeException(nameof(columnType), columnType, null);
            }
        }

        public override string GetSpecialDbType(SpecialDbTypes dbTypes)
        {
            switch (dbTypes)
            {
                case SpecialDbTypes.NTEXT:
                    return "text";
                case SpecialDbTypes.NCHAR:
                    return "character";
                default:
                    throw new ArgumentOutOfRangeException(nameof(dbTypes), dbTypes, null);
            }
        }

        public override string GetIndexType(IndexTypes indexTypes)
        {
            switch (indexTypes)
            {
                case IndexTypes.Clustered:
                    return "";
                    break;
                case IndexTypes.NonClustered:
                    return "";
                    break;
                case IndexTypes.UniqueNonClustered:
                    return "unique";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(indexTypes), indexTypes, null);
            }
            return base.GetIndexType(indexTypes);
        }

        public virtual string FormatPrimaryKey(TableDefinition table)
        {
            var columnDefinition = table.Columns.FirstOrDefault(x => x.IsPrimaryKey);
            if (columnDefinition == null)
                return string.Empty;

            var constraintName = string.IsNullOrEmpty(columnDefinition.PrimaryKeyName)
                ? $"PK_{table.Name}"
                : columnDefinition.PrimaryKeyName;

            var columns = string.IsNullOrEmpty(columnDefinition.PrimaryKeyColumns)
                ? GetQuotedColumnName(columnDefinition.Name)
                : string.Join(", ", columnDefinition.PrimaryKeyColumns
                                                    .Split(new[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries)
                                                    .Select(GetQuotedColumnName));

            var primaryKeyPart = "PRIMARY KEY";

            return string.Format(CreateConstraint,
                                 GetQuotedTableName(table.Name),
                                 GetQuotedName(constraintName),
                                 primaryKeyPart,
                                 columns);
        }

        public override string DeleteDefaultConstraint => "ALTER TABLE {0} DROP CONSTRAINT {2}";

        public override string DropIndex => "DROP INDEX {0} ON {1}";

        public override string RenameColumn => "sp_rename '{0}.{1}', '{2}', 'COLUMN'";

        public override bool SupportsIdentityInsert() => false;
        public override bool SupportsClustered() => false;
    }
}
