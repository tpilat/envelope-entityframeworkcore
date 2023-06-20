﻿using System.Collections.ObjectModel;

namespace Envelope.EntityFrameworkCore.Extensions;

public static class EnumerableExtensions
{
    public static ReadOnlyCollection<T> ToReadOnlyCollection<T>(this IEnumerable<T> sequence)
    {
        if (sequence == null)
        {
            return DefaultReadOnlyCollection<T>.Empty;
        }

        if (sequence is ReadOnlyCollection<T> onlys && onlys != null)
            return onlys;

        return new ReadOnlyCollection<T>(sequence.ToArray());
    }

    private static class DefaultReadOnlyCollection<T>
    {
        private static ReadOnlyCollection<T>? _defaultCollection;
        internal static ReadOnlyCollection<T> Empty => _defaultCollection ??= new ReadOnlyCollection<T>(Array.Empty<T>());
    }
}