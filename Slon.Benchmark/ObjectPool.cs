// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;

namespace Slon.Benchmark;

/// <summary>
/// Represents a policy for managing pooled objects.
/// </summary>
/// <typeparam name="T">The type of object which is being pooled.</typeparam>
public interface IPooledObjectPolicy<T> where T : notnull
{
    /// <summary>
    /// Create a <typeparamref name="T"/>.
    /// </summary>
    /// <returns>The <typeparamref name="T"/> which was created.</returns>
    T Create();

    /// <summary>
    /// Runs some processing when an object was returned to the pool. Can be used to reset the state of an object and indicate if the object should be returned to the pool.
    /// </summary>
    /// <param name="obj">The object to return to the pool.</param>
    /// <returns><see langword="true" /> if the object should be returned to the pool. <see langword="false" /> if it's not possible/desirable for the pool to keep the object.</returns>
    bool Return(T obj);
}

/// <summary>
/// Default implementation of <see cref="ObjectPool{T,TPolicy}"/>.
/// </summary>
/// <typeparam name="T">The type to pool objects for.</typeparam>
/// <typeparam name="TPolicy"></typeparam>
/// <remarks>This implementation keeps a cache of retained objects. This means that if objects are returned when the pool has already reached "maximumRetained" objects they will be available to be Garbage Collected.</remarks>
public class ObjectPool<T, TPolicy> where T : class where TPolicy : struct, IPooledObjectPolicy<T>
{
    TPolicy _policy;
    readonly int _maxCapacity;
    int _numItems;

    readonly ConcurrentQueue<T> _items = new();
    T? _fastItem;

    /// <summary>
    /// Creates an instance of <see cref="DefaultObjectPool{T}"/>.
    /// </summary>
    /// <param name="policy">The pooling policy to use.</param>
    public ObjectPool(TPolicy policy)
        : this(policy, Environment.ProcessorCount * 2)
    {
    }

    /// <summary>
    /// Creates an instance of <see cref="DefaultObjectPool{T}"/>.
    /// </summary>
    /// <param name="policy">The pooling policy to use.</param>
    /// <param name="maximumRetained">The maximum number of objects to retain in the pool.</param>
    public ObjectPool(TPolicy policy, int maximumRetained)
    {
        _policy = policy;
        _maxCapacity = maximumRetained - 1;  // -1 to account for _fastItem
    }

    /// <summary>
    /// Gets an object from the pool if one is available, otherwise creates one.
    /// </summary>
    /// <returns>A <typeparamref name="T"/>.</returns>
    public T Get()
    {
        var item = _fastItem;
        if (item == null || Interlocked.CompareExchange(ref _fastItem, null, item) != item)
        {
            if (_items.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _numItems);
                return item;
            }

            // no object available, so go get a brand new one
            return _policy.Create();
        }

        return item;
    }

    /// <summary>
    /// Return an object to the pool.
    /// </summary>
    /// <param name="obj">The object to add to the pool.</param>
    public void Return(T obj)
    {
        if (!_policy.Return(obj))
        {
            // policy says to drop this object
            return;
        }

        if (_fastItem != null || Interlocked.CompareExchange(ref _fastItem, obj, null) != null)
        {
            if (Interlocked.Increment(ref _numItems) <= _maxCapacity)
            {
                _items.Enqueue(obj);
                return;
            }

            // no room, clean up the count and drop the object on the floor
            Interlocked.Decrement(ref _numItems);
        }
    }
}
