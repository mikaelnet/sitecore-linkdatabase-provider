using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics;
using Sitecore.Diagnostics.PerformanceCounters;

namespace Sc.LinkDatabase
{
    public interface IBatchOperation
    {
        int Index { get; }
    }

    public class SqlBatchOperation : IBatchOperation
    {
        private const int MaxParametersInSqlRequest = 2100;
        private readonly object _lock = new object();

        private SqlDataApi DataApi { get; }

        private readonly StringBuilder _sqlStatements = new StringBuilder();
        private readonly List<object> _sqlParameters = new List<object>();
        private int _index;

        public int Index
        {
            get => _index;
            set
            {
                lock (_lock)
                {
                    _index = value;
                }
            }
        }

        public SqlBatchOperation(SqlDataApi dataApi)
        {
            DataApi = dataApi;
        }

        public void AddSqlBatchCommand(ISqlBatchCommand command)
        {
            int totalParameters;
            lock (_lock)
            {
                totalParameters = _sqlParameters.Count + command.QueryParameters.Count();
            }
            if (totalParameters > MaxParametersInSqlRequest)
                FlushBatches();

            lock (_lock)
            {
                _sqlStatements.AppendLine(command.QueryStatement);
                _sqlParameters.AddRange(command.QueryParameters);
                _index ++;
            }
        }

        public void FlushBatches()
        {
            try
            {
                string sql;
                object[] parameters;
                lock (_lock)
                {
                    sql = _sqlStatements.ToString();
                    parameters = _sqlParameters.ToArray();
                }

                // Don't execute empty statement list
                if (string.IsNullOrWhiteSpace(sql))
                    return;

                LogSqlOperation(sql, parameters);
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    DataApi.Execute(sql, parameters);
                });
                DataCount.LinksDataUpdated.IncrementBy(_index);
                DataCount.DataPhysicalWrites.Increment();
            }
            finally
            {
                ResetBatches();
            }
        }

        private void ResetBatches()
        {
            lock (_lock)
            {
                _sqlStatements.Clear();
                _sqlParameters.Clear();
                _index = 0;
            }
        }

        private void LogSqlOperation(string sql, IEnumerable<object> parameters)
        {
            if (!Log.IsDebugEnabled)
                return;

            var sw = new StringWriter();
            sw.WriteLine("Performing LinkDatabase batch operation:");
            sw.WriteLine("SQL statement:");
            sw.WriteLine(sql);
            sw.WriteLine("SQL Parameters:");
            var p = parameters.ToList();
            for (int i = 0; i < p.Count; i += 2)
            {
                sw.WriteLine("\t{0}: {1}", p[i], p[i+1]);
            }
            Log.Debug(sw.ToString(), nameof(SqlBatchOperation));
        }
    }
}
