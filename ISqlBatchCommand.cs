using System.Collections.Generic;

namespace Sc.LinkDatabase
{
    public interface ISqlBatchCommand
    {
        string QueryStatement { get; }
        IEnumerable<object> QueryParameters { get; }
    }
}
