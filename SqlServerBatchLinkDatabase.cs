using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sitecore;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.Diagnostics.PerformanceCounters;
using Sitecore.Globalization;
using Sitecore.Links;
using Sitecore.SecurityModel;

namespace Sc.LinkDatabase
{
    /// <summary>
    /// This is an optimized version of SqlServerLinkDatabase, combined with
    /// SqlServerBatchLinkDatabase without having the later ones drawbacks.
    /// Main features:
    /// * Multiple calls are batched into one, to reduce the number of
    ///   synchronous calls to SQL Server and thereby reduce latency
    /// * SQL operations only contains the changes, instead of removing
    ///   all links and then re-add them again
    /// * Since only changes are written to the Links table, its indexes
    ///   gets less fragmented. Fragmented indexes has been a huge issue
    ///   in other large applications
    /// * By default, Sitecore may add duplicates of the same link. For example
    ///   when the same component is used twice in a renderings field. Such
    ///   links are now only added once
    /// * When rebuilding the links table, its content stay intact. This is
    ///   important when running scheduled operations, integration code etc.
    ///   Sitecore's SqlServerBatchLinkDatabase doesn't keep it intact,
    ///   making it basically useless in many scenarios.
    /// </summary>
    /// <remarks>
    /// The base class <see cref="Sitecore.Links.LinkDatabase"/> hides some
    /// protected virtual methods that must not be called. Those are overridden
    /// and deliberately throws exceptions if ever called.
    /// </remarks>
    [UsedImplicitly]
    public class SqlServerBatchLinkDatabase : Sitecore.Links.LinkDatabase
    {
        private const int DatabaseNameLength = 150;
        private const int LanguageNameLength = 50;
        private readonly LockSet _locks = new LockSet();

        private const string LinksTableSqlColumns =
            "[ID], [SourceDatabase], [SourceItemID], [SourceLanguage], [SourceVersion], [SourceFieldID], " +
            "[TargetDatabase], [TargetItemID], [TargetLanguage], [TargetVersion], [TargetPath]";

        private SqlDataApi DataApi { get; }

        public SqlServerBatchLinkDatabase(SqlDataApi dataApi)
        {
            Assert.ArgumentNotNull(dataApi, nameof(dataApi));
            DataApi = dataApi;
        }


        #region Compact links database

        /// <summary>
        /// Removes links where the target no longer exists.
        /// Implementation taken from SqlServerLinkDatabase
        /// </summary>
        /// <remarks>Improved from SqlServerLinkDatabase</remarks>
        /// <param name="database"></param>
        public override void Compact(Database database)
        {
            Assert.ArgumentNotNull(database, nameof(database));

            int batchSize = Settings.LinkDatabase.MaximumBatchSize;
            var lastProcessed = ID.Null;
            do
            {
                lastProcessed = BatchCompact(database, batchSize, lastProcessed);
            } while (lastProcessed != ID.Null);
        }

        private ID BatchCompact(Database database, int batchSize, ID lastProcessed)
        {
            Assert.ArgumentNotNull(database, nameof(database));

            var selectSql = $@"
SELECT TOP {batchSize} [ID], [SourceItemID], [SourceLanguage], [SourceVersion] 
FROM [Links] WITH (NOLOCK) 
WHERE [SourceDatabase]=@{LinksTableColumns.Database}";
            if (lastProcessed != ID.Null)
                selectSql += $" AND [ID] > @{nameof(lastProcessed)}";
            selectSql += " ORDER BY [ID]";

            var linkList = new List<Tuple<Guid,ID,Language, Sitecore.Data.Version>>();
            using (var reader = DataApi.CreateReader(selectSql, 
                LinksTableColumns.Database, GetString(database.Name, DatabaseNameLength), 
                nameof(lastProcessed), lastProcessed))
            {
                DataCount.LinksDataRead.Increment();
                while (reader.Read())
                {
                    var id = DataApi.GetGuid(0, reader);
                    var itemId = DataApi.GetId(1, reader);
                    var language = DataApi.GetLanguage(2, reader);
                    var version = DataApi.GetVersion(3, reader);
                    linkList.Add(new Tuple<Guid, ID, Language, Sitecore.Data.Version>(id, itemId, language, version));
                }
            }

            var batchOperation = new SqlBatchOperation(DataApi);
            Guid lastId = Guid.Empty;
            foreach (var row in linkList)
            {
                lastId = row.Item1;
                if (ItemExists(row.Item2, null, row.Item3, row.Item4, database))
                    continue;

                var command = LinkDatabaseCommand.CreateDeleteCommand(row.Item1, batchOperation);
                batchOperation.AddSqlBatchCommand(command);
            }

            batchOperation.FlushBatches();
            return new ID(lastId);
        }

        #endregion

        #region GetBrokenLinks

        /// <summary>
        /// Gets any broken links in the database
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        /// <remarks>Same as SqlLinkDatabase</remarks>
        public override ItemLink[] GetBrokenLinks(Database database)
        {
            Assert.ArgumentNotNull(database, nameof(database));

            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links]
WHERE [SourceDatabase]=@{LinksTableColumns.Database}
ORDER BY [SourceItemID], [SourceFieldID]";

            var databaseName = GetString(database.Name, DatabaseNameLength);
            using (DataProviderReader reader = DataApi.CreateReader(sql, LinksTableColumns.Database, databaseName))
            {
                DataCount.LinksDataRead.Increment();
                var links = FindBrokenLinks(reader);
                return links.ToArray();
            }
        }

        private List<ItemLink> FindBrokenLinks(DataProviderReader reader)
        {
            var links = new List<ItemLink>();
            using (new SecurityDisabler())
            {
                while (reader.Read())
                {
                    var itemLink = MapLinksRow(reader).Item2;

                    var targetDatabase = Factory.GetDatabase(itemLink.TargetDatabaseName);
                    if (!ItemExists(itemLink.TargetItemID, itemLink.TargetPath, itemLink.TargetItemLanguage, itemLink.TargetItemVersion, targetDatabase))
                    {
                        // Note: The original Sitecore source code adds "sourceDatabaseName" twice instead of using targetDatabaseName
                        links.Add(itemLink);
                    }

                    var job = Context.Job;
                    if (job != null && job.Category == "GetBrokenLinks")
                    {
                        job.Status.Processed ++;
                    }

                    DataCount.LinksDataRead.Increment();
                    DataCount.DataPhysicalReads.Increment();
                }
            }

            return links;
        }

        #endregion

        #region GetReferences methods

        /// <summary>
        /// Gets the number of references to this item
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        /// <remarks>Same as SqlLinkDatabase</remarks>
        public override int GetReferenceCount(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));

            var sql = $@"
SELECT COUNT(*) 
FROM [Links] WITH (NOLOCK, INDEX = ndxSourceItemID)
WHERE [SourceDatabase]=@{LinksTableColumns.Database}
AND [SourceItemID]=@{LinksTableColumns.ItemID}";
            var databaseName = GetString(item.Database.Name, DatabaseNameLength);
            using (var reader = DataApi.CreateReader(sql, LinksTableColumns.Database, databaseName, LinksTableColumns.ItemID, item.ID.ToGuid()))
            {
                DataCount.LinksDataRead.Increment();
                if (reader.Read())
                {
                    return DataApi.GetInt(0, reader);
                }
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        /// <remarks>Same as SqlLinkDatabase</remarks>
        public override ItemLink[] GetReferences(Item item)
        {
            return GetReferencesTuples(item).Select(link => link.Item2).ToArray();
        }

        private List<Tuple<Guid, ItemLink>> GetReferencesTuples(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));

            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links] WITH (INDEX = ndxSourceItemID)
WHERE [SourceDatabase]=@{LinksTableColumns.Database} 
AND [SourceItemID]=@{LinksTableColumns.ItemID}";

            return GetLinks(sql, item);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        /// <remarks>Same as SqlLinkDatabase</remarks>
        public override ItemLink[] GetItemVersionReferences(Item version)
        {
            Assert.ArgumentNotNull(version, nameof(version));
            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links] WITH (INDEX = ndxSourceItemID)
WHERE [SourceDatabase]=@{LinksTableColumns.Database}
AND [SourceItemID]=@{LinksTableColumns.ItemID}
AND [SourceLanguage]=@{LinksTableColumns.SourceLanguage}
AND [SourceVersion]=@{LinksTableColumns.SourceVersion}";

            return GetLinks(sql, version, new object[] {
                    LinksTableColumns.Database, GetString(version.Database.Name, DatabaseNameLength),
                    LinksTableColumns.ItemID, version.ID.ToGuid(),
                    LinksTableColumns.SourceLanguage, GetString(version.Language.ToString(), LanguageNameLength),
                    LinksTableColumns.SourceVersion, version.Version.Number
                })
                .Select(link => link.Item2).ToArray();
        }

        #endregion

        #region GetReferrers methods

        public override int GetReferrerCount(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));

            var sql = $@"
SELECT COUNT(*) 
FROM [Links] WITH (NOLOCK, INDEX = ndxTargetItemID)
WHERE [TargetDatabase]=@{LinksTableColumns.Database}
AND [TargetItemID]=@{LinksTableColumns.ItemID}";
            var databaseName = GetString(item.Database.Name, DatabaseNameLength);
            using (var reader = DataApi.CreateReader(sql, LinksTableColumns.Database, databaseName, LinksTableColumns.ItemID, item.ID.ToGuid()))
            {
                DataCount.LinksDataRead.Increment();
                if (reader.Read())
                    return DataApi.GetInt(0, reader);
            }
            return 0;
        }

        public override ItemLink[] GetReferrers(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));

            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links] WITH (NOLOCK, INDEX = ndxTargetItemID) 
WHERE [TargetDatabase]=@{LinksTableColumns.Database}
AND [TargetItemID]=@{LinksTableColumns.ItemID}";
            return GetLinks(sql, item).Select(link => link.Item2).ToArray();
        }

        public override ItemLink[] GetReferrers(Item item, ID sourceFieldId)
        {
            Assert.ArgumentNotNull(item, nameof(item));
            Assert.ArgumentNotNull(sourceFieldId, nameof(sourceFieldId));

            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links] WITH (NOLOCK, INDEX = ndxTargetItemID) 
WHERE [TargetDatabase]=@{LinksTableColumns.Database}
AND [TargetItemID]=@{LinksTableColumns.ItemID}
AND [SourceFieldID]=@{LinksTableColumns.FieldID}";
            return GetLinks(sql, item, sourceFieldId).Select(link => link.Item2).ToArray();
        }

        public override ItemLink[] GetItemVersionReferrers(Item version)
        {
            Assert.ArgumentNotNull(version, nameof(version));
            
            var sql = $@"
SELECT {LinksTableSqlColumns}
FROM [Links] WITH (NOLOCK, INDEX = ndxTargetItemID) 
WHERE [TargetDatabase]=@{LinksTableColumns.Database}
AND [TargetItemID]=@{LinksTableColumns.ItemID}
AND [TargetLanguage]=@{LinksTableColumns.TargetLanguage} 
AND [TargetVersion]=@{LinksTableColumns.TargetVersion}";

            return GetLinks(sql, version, new object[] {
                    LinksTableColumns.Database, GetString(version.Database.Name, DatabaseNameLength),
                    LinksTableColumns.ItemID, version.ID.ToGuid(),
                    LinksTableColumns.TargetLanguage, GetString(version.Language.ToString(), LanguageNameLength),
                    LinksTableColumns.TargetVersion, version.Version.Number
                })
                .Select(link => link.Item2).ToArray();
        }

        #endregion

        #region UpdateLinks

        public override void RemoveReferences(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));
            
            var sql = $@"
DELETE FROM [Links] 
WHERE [SourceDatabase]=@{LinksTableColumns.Database} 
AND [SourceItemID]=@{LinksTableColumns.ItemID}";
            
            DataApi.Execute(sql,
                LinksTableColumns.Database, GetString(item.Database.Name, DatabaseNameLength),
                LinksTableColumns.ItemID, item.ID.ToGuid());
            
            DataCount.LinksDataUpdated.Increment();
        }

        private void RemoveLink(SqlBatchOperation batch, Guid itemLinkId)
        {
            var removeCommand = LinkDatabaseCommand.CreateDeleteCommand(itemLinkId, batch);
            batch.AddSqlBatchCommand(removeCommand);
        }

        private void AddLink(SqlBatchOperation batch, Item item, ItemLink link)
        {
            var insertCommand = LinkDatabaseCommand.CreateInsertCommand(item.Database.Name, item.ID, link.SourceItemLanguage, 
                link.SourceItemVersion, link.SourceFieldID, link.TargetDatabaseName, link.TargetItemID, 
                link.TargetItemLanguage, link.TargetItemVersion, link.TargetPath, batch);

            batch.AddSqlBatchCommand(insertCommand);
        }

        public override void UpdateItemVersionReferences(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));

            Task.Factory.StartNew(() => UpdateItemVersionLinks(item, item.Links.GetAllLinks(false)));
        }

        /// <summary>
        /// Batch improved version
        /// </summary>
        /// <param name="item"></param>
        /// <param name="links"></param>
        /// <remarks>Same as SqlLinkDatabase</remarks>
        protected override void UpdateLinks(Item item, ItemLink[] links)
        {
            Assert.ArgumentNotNull(item, nameof(item));
            Assert.ArgumentNotNull(links, nameof(links));

            UpdateLinks(item, links, () => GetReferencesTuples(item));
        }

        protected override void UpdateItemVersionLinks(Item item, ItemLink[] links)
        {
            Assert.ArgumentNotNull(item, nameof(item));
            Assert.ArgumentNotNull(links, nameof(links));

            UpdateLinks(item, links, () =>
            {
                return GetReferencesTuples(item).Where(tuple =>
                {
                    var il = tuple.Item2;
                    if (il.SourceItemID.IsNull)
                        return false;
                    return il.SourceItemLanguage == Language.Invariant && il.SourceItemVersion == Sitecore.Data.Version.Latest ||
                           il.SourceItemLanguage == item.Language && il.SourceItemVersion == Sitecore.Data.Version.Latest ||
                           il.SourceItemLanguage == item.Language && il.SourceItemVersion == item.Version;
                });
            });
        }

        private void UpdateLinks(Item item, IEnumerable<ItemLink> newLinks, Func<IEnumerable<Tuple<Guid,ItemLink>>> existingLinks)
        {
            lock (_locks.GetLock(item.ID))
            {
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    using (DataProviderTransaction transaction = DataApi.CreateTransaction())
                    {
                        var batch = new SqlBatchOperation(DataApi);
                        var linksToProcess = existingLinks().ToList();

                        foreach (var link in FilterItemLinks(newLinks))
                        {
                            var existingLink = linksToProcess.FirstOrDefault(l => ItemLinksAreEqual(l.Item2, link));
                            if (existingLink != null)
                            {
                                linksToProcess.Remove(existingLink);
                                continue;
                            }

                            if (link.SourceItemID.IsNull)
                                continue;

                            AddLink(batch, item, link);
                        }

                        foreach (var existingLinkId in linksToProcess.Select(l => l.Item1))
                        {
                            RemoveLink(batch, existingLinkId);
                        }

                        batch.FlushBatches();
                        transaction.Complete();
                    }
                });
            }
        }

        #endregion

        #region Obsolete methods

#pragma warning disable CS0809 // Obsolete member overrides non-obsolete member
        // The protected methods below are implemented in the base class. They should
        // never be called as they don't utilize the batch methods. They're overridden
        // here just to ensure no future change starts using them.

        /// <inheritdoc />
        [Obsolete("Never call this method", true)]
        protected override void AddLink(Item item, ItemLink link) => throw new InvalidOperationException("This method should never be called");

        /// <inheritdoc />
        [Obsolete("Never call this method", true)]
        protected override void AddModifiedItemLinks(Item item, ItemLink[] itemLinkReferences, ItemLink[] contextItemLinks) => throw new InvalidOperationException("This method should never be called");

        /// <inheritdoc />
        [Obsolete("Never call this method", true)]
        protected override void RemoveItemVersionLink(ItemLink itemLink) => throw new InvalidOperationException("This method should never be called");

        /// <inheritdoc />
        [Obsolete("Never call this method", true)]
        protected override void RemoveDeletedItemLinks(ItemLink[] itemLinkReferences, ItemLink[] contextItemLinks) => throw new InvalidOperationException("This method should never be called");

#pragma warning restore CS0809 // Obsolete member overrides non-obsolete member

        #endregion

        #region (private) GetLinks

        /// <summary>
        /// Assumes "database" and "itemID" is present in the where clause
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="item"></param>
        /// <returns></returns>
        private List<Tuple<Guid, ItemLink>> GetLinks(string sql, Item item) => GetLinks(sql, item, new object[] {
            LinksTableColumns.Database, GetString(item.Database.Name, DatabaseNameLength),
            LinksTableColumns.ItemID, item.ID.ToGuid()
        });

        /// <summary>
        /// Assumes "database", "itemID" and "fieldID" is present in the where clause
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="item"></param>
        /// <param name="sourceFieldId"></param>
        /// <returns></returns>
        private List<Tuple<Guid, ItemLink>> GetLinks(string sql, Item item, ID sourceFieldId)
        {
            Assert.ArgumentNotNull(sql, nameof(sql));
            Assert.ArgumentNotNull(item, nameof(item));
            Assert.ArgumentNotNull(sourceFieldId, nameof(sourceFieldId));

            return GetLinks(sql, item, new object[] {
                LinksTableColumns.Database, GetString(item.Database.Name, DatabaseNameLength),
                LinksTableColumns.ItemID, item.ID.ToGuid(),
                LinksTableColumns.FieldID, sourceFieldId.ToGuid()
            });
        }

        private List<Tuple<Guid,ItemLink>> GetLinks(string sql, Item item, object[] parameters)
        {
            var itemLinkList = new List<Tuple<Guid,ItemLink>>();
            lock (_locks.GetLock(item.ID))
            {
                using (DataProviderReader reader = DataApi.CreateReader(sql, parameters))
                {
                    while (reader.Read())
                    {
                        itemLinkList.Add(MapLinksRow(reader));
                    }

                    DataCount.LinksDataRead.IncrementBy(itemLinkList.Count);
                    DataCount.DataPhysicalReads.Increment();
                }
            }

            return itemLinkList;
        }

        private bool ItemLinksAreEqual(ItemLink left, ItemLink right)
        {
            if (left == null && right == null)
                return true;

            if (left == null || right == null)
                return false;

            if (!string.Equals(left.SourceDatabaseName, right.SourceDatabaseName, StringComparison.Ordinal) ||
                left.SourceItemID != right.SourceItemID ||
                left.SourceFieldID != right.SourceFieldID ||
                left.SourceItemLanguage != right.SourceItemLanguage ||
                left.SourceItemVersion != right.SourceItemVersion)
                return false;

            if (!string.Equals(left.TargetDatabaseName, right.TargetDatabaseName, StringComparison.Ordinal) ||
                left.TargetItemID != right.TargetItemID ||
                left.TargetItemLanguage != right.TargetItemLanguage ||
                left.TargetItemVersion != right.TargetItemVersion ||
                !string.Equals(left.TargetPath, right.TargetPath, StringComparison.Ordinal)
            )
                return false;

            return true;
        }

        private Tuple<Guid, ItemLink> MapLinksRow(DataProviderReader reader)
        {
            var id = DataApi.GetGuid(0, reader);
            var sourceDatabaseName = DataApi.GetString(1, reader);
            var sourceItemID = DataApi.GetId(2, reader);
            var sourceItemLanguage = DataApi.GetLanguage(3, reader);
            var sourceItemVersion = DataApi.GetVersion(4, reader);
            var sourceFieldID = DataApi.GetId(5, reader);

            var targetDatabaseName = DataApi.GetString(6, reader);
            var targetItemID = DataApi.GetId(7, reader);
            var targetItemLanguage = DataApi.GetLanguage(8, reader);
            var targetItemVersion = DataApi.GetVersion(9, reader);
            var targetPath = DataApi.GetString(10, reader);

            var link = new ItemLink(sourceDatabaseName, sourceItemID, sourceItemLanguage, sourceItemVersion, sourceFieldID,
                targetDatabaseName, targetItemID, targetItemLanguage, targetItemVersion, targetPath);
            return new Tuple<Guid, ItemLink>(id, link);
        }

        private IEnumerable<ItemLink> FilterItemLinks(IEnumerable<ItemLink> itemLinks)
        {
            var processed = new HashSet<ItemLink>();
            foreach (var itemLink in itemLinks)
            {
                if (processed.Any(l => ItemLinksAreEqual(l, itemLink)))
                    continue;
                
                processed.Add(itemLink);
                yield return itemLink;
            }
        }

        #endregion
    }


}