using System;
using System.Data;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Text.RegularExpressions;

public class Django
{
    [Microsoft.SqlServer.Server.SqlFunction]
    public static int Regex(string ev, string pattern)
    {
        if (ev == null || pattern == null) {
            return 0;
        }
		Regex r = new Regex(pattern);
		return r.IsMatch(ev) ? 1 : 0;
    }

    [Microsoft.SqlServer.Server.SqlFunction]
    public static int IRegex(string ev, string pattern)
    {
        if (ev == null || pattern == null) {
            return 0;
        }
		Regex r = new Regex(pattern, RegexOptions.IgnoreCase);
		return r.IsMatch(ev) ? 1 : 0;
    }
}
