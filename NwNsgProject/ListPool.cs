// Copyright (c) Microsoft.  All Rights Reserved.  Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

using System.Collections.Generic;

namespace Microsoft.CodeAnalysis.Formatting
{
    internal static class ListPool<T>
    {
        public static List<T> Allocate()
        {
            return SharedPools.Default<List<T>>().AllocateAndClear();
        }

        public static void Free(List<T> list)
        {
            SharedPools.Default<List<T>>().ClearAndFree(list);
        }

        public static List<T> ReturnAndFree(List<T> list)
        {
            SharedPools.Default<List<T>>().ForgetTrackedObject(list);
            return list;
        }
    }
}

class ThrowTest3
{
    static void ProcessString(string s)
    {
        if (s == null)
        {
            throw new ArgumentNullException();
        }
    }

    static void Main()
    {
        try
        {
            string s = null;
            ProcessString(s);
        }
        // Most specific:
        catch (ArgumentNullException e)
        {
            Console.WriteLine("{0} First exception caught.", e);
        }
        // Least specific:
        catch (Exception e)
        {
            Console.WriteLine("{0} Second exception caught.", e);
        }
    }
}
/*
 Output:
 System.ArgumentNullException: Value cannot be null.
 at Test.ThrowTest3.ProcessString(String s) ... First exception caught.
*/
