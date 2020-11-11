using System;
using System.Collections.Generic;
using Sitecore.Data;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Sitecore.Links;

namespace Sc.LinkDatabase
{
    public sealed class LinkDatabaseCommand : ISqlBatchCommand
    {
        private const int DatabaseNameLength = 150;
        private const int LanguageNameLength = 50;

        public string QueryStatement { get; }
        public IEnumerable<object> QueryParameters { get; }

        private LinkDatabaseCommand(string queryStatement, IEnumerable<object> queryParameters)
        {
            QueryStatement = queryStatement;
            QueryParameters = queryParameters;
        }

        public static LinkDatabaseCommand CreateInsertCommand(ItemLink itemLink, IBatchOperation operation)
        {
            return CreateInsertCommand(
                itemLink.SourceDatabaseName, itemLink.SourceItemID, itemLink.SourceItemLanguage, 
                itemLink.SourceItemVersion, itemLink.SourceFieldID, itemLink.TargetDatabaseName, 
                itemLink.TargetItemID, itemLink.TargetItemLanguage, itemLink.TargetItemVersion, 
                itemLink.TargetPath, operation
            );
        }

        public static LinkDatabaseCommand CreateInsertCommand(
            string sourceDatabase, ID sourceItemID, Language sourceLanguage, Sitecore.Data.Version sourceVersion, ID sourceFieldID,
            string targetDatabase, ID targetItemID, Language targetLanguage, Sitecore.Data.Version targetVersion, string targetPath,
            IBatchOperation operation)
        {
            var parameters = new List<object>();
            var index = operation.Index;
            var sql = $@"
INSERT INTO [Links] (
    [SourceDatabase], [SourceItemID], [SourceLanguage], [SourceVersion], [SourceFieldID], 
    [TargetDatabase], [TargetItemID], [TargetLanguage], [TargetVersion], [TargetPath]
) VALUES(
    @{LinksTableColumns.Database}{index}, @{LinksTableColumns.ItemID}{index}, 
    @{LinksTableColumns.SourceLanguage}{index}, @{LinksTableColumns.SourceVersion}{index}, 
    @{LinksTableColumns.FieldID}{index}, @{LinksTableColumns.TargetDatabase}{index}, 
    @{LinksTableColumns.TargetID}{index}, @{LinksTableColumns.TargetLanguage}{index}, 
    @{LinksTableColumns.TargetVersion}{index}, @{LinksTableColumns.TargetPath}{index}
)";
            
            AddToParameters(parameters, $"{LinksTableColumns.Database}{index}", GetString(sourceDatabase, DatabaseNameLength));
            AddToParameters(parameters, $"{LinksTableColumns.ItemID}{index}", sourceItemID.Guid);
            AddToParameters(parameters, $"{LinksTableColumns.SourceLanguage}{index}", GetString(sourceLanguage.ToString(), LanguageNameLength));
            AddToParameters(parameters, $"{LinksTableColumns.SourceVersion}{index}", sourceVersion.Number);
            AddToParameters(parameters, $"{LinksTableColumns.FieldID}{index}", sourceFieldID.Guid);

            AddToParameters(parameters, $"{LinksTableColumns.TargetDatabase}{index}", GetString(targetDatabase, DatabaseNameLength));
            AddToParameters(parameters, $"{LinksTableColumns.TargetID}{index}", targetItemID.Guid);
            AddToParameters(parameters, $"{LinksTableColumns.TargetLanguage}{index}", GetString(targetLanguage.ToString(), LanguageNameLength));
            AddToParameters(parameters, $"{LinksTableColumns.TargetVersion}{index}", targetVersion.Number);
            AddToParameters(parameters, $"{LinksTableColumns.TargetPath}{index}", targetPath);

            return new LinkDatabaseCommand(sql, parameters);
        }

        public static LinkDatabaseCommand CreateDeleteCommand(Guid itemLinkId, IBatchOperation operation)
        {
            var paramName = $"id{operation.Index}";
            var parameters = new List<object>();
            var sql = $"DELETE FROM [Links] WHERE [ID]=@{paramName}";
            AddToParameters(parameters, paramName, itemLinkId);

            return new LinkDatabaseCommand(sql, parameters);
        }

        public static LinkDatabaseCommand CreateDeleteCommand(ItemLink itemLink, IBatchOperation operation)
        {
            var parameters = new List<object>();
            var index = operation.Index;
            var sql = $@"
DELETE  FROM [Links]
WHERE [SourceDatabase]=@{LinksTableColumns.Database}{index}
AND [SourceItemID]=@{LinksTableColumns.ItemID}{index}
AND [SourceLanguage]=@{LinksTableColumns.SourceLanguage}{index}
AND [SourceVersion]=@{LinksTableColumns.SourceVersion}{index}
AND [SourceFieldID]=@{LinksTableColumns.FieldID}{index}
AND [TargetDatabase]=@{LinksTableColumns.TargetDatabase}{index}
AND [TargetItemID]=@{LinksTableColumns.TargetID}{index}
AND [TargetLanguage]=@{LinksTableColumns.TargetLanguage}{index}
AND [TargetVersion] = @{LinksTableColumns.TargetVersion}{index}";

            AddToParameters(parameters, LinksTableColumns.Database, GetString(itemLink.SourceDatabaseName, DatabaseNameLength));
            AddToParameters(parameters, LinksTableColumns.ItemID, itemLink.SourceItemID.Guid);
            AddToParameters(parameters, LinksTableColumns.SourceLanguage, GetString(itemLink.SourceItemLanguage.ToString(), LanguageNameLength));
            AddToParameters(parameters, LinksTableColumns.SourceVersion, itemLink.SourceItemVersion.Number);
            AddToParameters(parameters, LinksTableColumns.FieldID, itemLink.SourceFieldID);
            AddToParameters(parameters, LinksTableColumns.TargetDatabase, GetString(itemLink.TargetDatabaseName, DatabaseNameLength));
            AddToParameters(parameters, LinksTableColumns.TargetID, itemLink.TargetItemID);
            AddToParameters(parameters, LinksTableColumns.TargetLanguage, GetString(itemLink.TargetItemLanguage.ToString(), LanguageNameLength));
            AddToParameters(parameters, LinksTableColumns.TargetVersion, itemLink.TargetItemVersion.Number);

            return new LinkDatabaseCommand(sql, parameters);
        }

        private static void AddToParameters(ICollection<object> parameters, string name, object value)
        {
            parameters.Add(name);
            parameters.Add(value);
        }

        private static string GetString(string value, int maxLength)
        {
            if (value.Length <= maxLength)
                return value;

            var str = value.Substring(0, maxLength);
            Log.Warn("A string had to be truncated before being written to the link database.", nameof(LinkDatabaseCommand));
            Log.Warn($"Original value: '{value}'.", nameof(LinkDatabaseCommand));
            Log.Warn($"Truncated value: '{str}'.", nameof(LinkDatabaseCommand));
            return value.Substring(0, maxLength);
        }
    }
}
