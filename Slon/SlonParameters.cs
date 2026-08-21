using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Slon;

/// <summary>
/// Represents a collection of parameters relevant to a <see cref="Slon.SlonCommand"/> or <see cref="Slon.SlonBatchCommand"/> as well as their respective mappings to columns in
/// a <see cref="DataSet"/>.
/// </summary>
public sealed partial class SlonParameters : IList<SlonParameter>
{
    /// Initializes an empty parameter collection.
    public SlonParameters() : this(initialCapacity: 5) {}

    /// <summary>Initializes a parameter collection from parameter names and values.</summary>
    /// <param name="parameters">The parameters to add.</param>
    public SlonParameters(IEnumerable<KeyValuePair<string, object?>> parameters) : this(initialCapacity: 5)
    {
        foreach (var (key, value) in parameters)
            Add(key, value);
    }

    /// <inheritdoc cref="IEnumerable{T}.GetEnumerator"/>
    internal Enumerator GetStructEnumerator() => new(this);

    /// <inheritdoc />
    internal struct Enumerator : IEnumerator<KeyValuePair<string, object?>>
    {
        NameValueEnumerator _enumerator;

        Enumerator(NameValueEnumerator enumerator)
        {
            _enumerator = enumerator;
            _enumerator.Reset();
        }

        internal Enumerator(SlonParameters collection) => _enumerator = collection.GetNameValueEnumerator();

        // Just here to allow GetStructEnumerator to be used with foreach.
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Enumerator GetEnumerator() => new(_enumerator);

        /// <inheritdoc />
        public bool MoveNext() => _enumerator.MoveNext();
        /// <inheritdoc />
        public KeyValuePair<string, object?> Current => _enumerator.Current;
        /// <inheritdoc />
        public void Dispose() => _enumerator.Dispose();
        /// <inheritdoc />
        void IEnumerator.Reset() => _enumerator.Reset();
        /// <inheritdoc />
        object IEnumerator.Current => Current;
    }

    /// Creates an empty parameter collection.
    public static SlonParameters Create() => new();

    /// <summary>Creates a collection containing one positional parameter.</summary>
    /// <param name="value">The parameter value.</param>
    public static SlonParameters Create(object? value) => Create(new(PositionalName, value));

    /// <summary>Creates a collection containing one named parameter.</summary>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    public static SlonParameters Create(string parameterName, object? value) => Create(new(parameterName, value));

    /// <summary>Creates a collection containing one parameter.</summary>
    /// <param name="parameter">The parameter name and value.</param>
    public static SlonParameters Create(KeyValuePair<string, object?> parameter) => new() { { parameter.Key, parameter.Value } };

    /// <summary>Creates a collection containing the supplied parameters.</summary>
    /// <param name="parameters">The parameters to add.</param>
    public static SlonParameters CreateRange(params IEnumerable<KeyValuePair<string, object?>> parameters) => new(parameters);

    /// <summary>Creates a collection containing the supplied parameters.</summary>
    /// <param name="parameters">The parameters to add.</param>
    public static SlonParameters CreateRange(params ReadOnlySpan<KeyValuePair<string, object?>> parameters)
    {
        var collection = new SlonParameters(parameters.Length);
        foreach (var (key, value) in parameters)
            collection.Add(key, value);

        return collection;
    }

    /// <summary>Ensures that the capacity of this list is at least the specified <paramref name="capacity" />.</summary>
    /// <param name="capacity">The minimum capacity to ensure.</param>
    /// <returns>The new capacity of this list.</returns>
    public int EnsureCapacity(int capacity) => _parameters.EnsureCapacity(capacity);

    /// <summary>Adds the parameter names and values of the specified enumerable to the collection.</summary>
    /// <param name="parameters">The parameters to add to the collection.</param>
    public void AddRange(params IEnumerable<KeyValuePair<string, object?>> parameters)
    {
        foreach (var value in parameters)
            Add(value);
    }

    /// <summary>Adds the parameter values of the specified enumerable to the collection.</summary>
    /// <param name="parameters">The parameters to add to the collection.</param>
    public void AddRange(params ReadOnlySpan<object?> parameters)
    {
        foreach (var value in parameters)
            Add(value);
    }

    /// <summary>Adds the parameter names and values of the specified span to the collection.</summary>
    /// <param name="parameters">The parameters to add to the collection.</param>
    public void AddRange(params ReadOnlySpan<KeyValuePair<string, object?>> parameters)
    {
        foreach (var (key, value) in parameters)
            Add(key, value);
    }

    /// <summary>Adds a parameter value with the given name.</summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(string parameterName, T? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        AddCore(parameterName, value);
    }

    /// <summary>Adds a parameter value with the given name and DbType.</summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">The DbType for the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(string parameterName, DbType dbType, T? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        var parameter = new SlonParameter<T>(value)
        {
            DbType = dbType
        };
        AddCore(parameterName, parameter);
    }

    /// <summary>Adds a parameter value.</summary>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(T? value) => AddCore(PositionalName, value);

    /// <summary>Adds a parameter value.</summary>
    /// <param name="dbType">The <see cref="System.Data.DbType"/> for the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(DbType dbType, T? value)
    {
        var parameter = new SlonParameter<T>(value)
        {
            DbType = dbType
        };
        AddCore(null, parameter);
    }

    /// <summary>Adds a parameter value with the given <see cref="Slon.SlonDbType"/>.</summary>
    /// <param name="dbType">The <see cref="Slon.SlonDbType"/> for the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(SlonDbType dbType, T? value)
    {
        var parameter = new SlonParameter<T>(value)
        {
            SlonDbType = dbType
        };
        AddCore(null, parameter);
    }

    /// <summary>Adds a parameter value with the given name and <see cref="Slon.SlonDbType"/>.</summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">The <see cref="Slon.SlonDbType"/> for the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    /// <typeparam name="T">The type of value.</typeparam>
    public void Add<T>(string parameterName, SlonDbType dbType, T? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        var parameter = new SlonParameter<T>(parameterName, value)
        {
            SlonDbType = dbType
        };
        AddCore(parameterName, parameter);
    }

    bool TryGetValueCore(string parameterName, [NotNullWhen(true)]out SlonParameter? parameter)
    {
        var index = IndexOfCore(parameterName);

        if (index == -1)
        {
            parameter = null;
            return false;
        }

        parameter = GetOrAddParameterInstance(index);
        return true;
    }

    /// <inheritdoc />
    IEnumerator<SlonParameter> IEnumerable<SlonParameter>.GetEnumerator()
    {
        for (var i = 0; i < Count; i++)
            yield return GetOrAddParameterInstance(i);
    }

    /// <summary>Gets the <see cref="SlonParameter"/> with the specified name.</summary>
    /// <param name="parameterName">The name of the <see cref="SlonParameter"/> to retrieve.</param>
    /// <value>
    /// The <see cref="SlonParameter"/> with the specified name, or a <see langword="null"/> reference if the parameter is not found.
    /// </value>
    public new SlonParameter this[string parameterName]
    {
        get
        {
            ArgumentNullException.ThrowIfNull(parameterName);
            if (!TryGetValueCore(parameterName, out SlonParameter? parameter))
                throw new ArgumentException("Parameter was not found.");

            return parameter;
        }
        set
        {
            ArgumentNullException.ThrowIfNull(parameterName);
            ArgumentNullException.ThrowIfNull(value);

            var index = IndexOfCore(parameterName);
            if (index == -1)
                AddCore(parameterName, value);
            else
                ReplaceCore(index, parameterName, value);
        }
    }

    /// <summary>Gets the <see cref="SlonParameter"/> at the specified index.</summary>
    /// <param name="index">The zero-based index of the <see cref="SlonParameter"/> to retrieve.</param>
    /// <value>The <see cref="SlonParameter"/> at the specified index.</value>
    public new SlonParameter this[int index]
    {
        get
        {
            if ((uint)index >= Count)
                throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than or equal to Count.");

            return GetOrAddParameterInstance(index);
        }
        set
        {
            if ((uint)index >= Count)
                throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than or equal to Count.");

            if (value is null)
                throw new ArgumentNullException(nameof(value));

            ReplaceCore(index, value.ParameterName, value);
        }
    }

    /// <summary>Gets a value indicating whether a <see cref="Slon.SlonParameter"/> with the specified name exists in the collection.</summary>
    /// <param name="parameterName">The name of the <see cref="Slon.SlonParameter"/> object to find.</param>
    /// <param name="parameter">
    /// A reference to the requested parameter is returned if it is found in the list.
    /// This value is <see langword="null"/> if the parameter is not found.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the collection contains the parameter and param will contain the parameter.
    /// Otherwise, <see langword="false"/>.
    /// </returns>
    public bool TryGetValue(string parameterName, [NotNullWhen(true)] out SlonParameter? parameter)
    {
        ArgumentNullException.ThrowIfNull(parameterName);
        return TryGetValueCore(parameterName, out parameter);
    }

    /// <inheritdoc />
    void ICollection<SlonParameter>.Add(SlonParameter item) => AddCore(null, item ?? throw new ArgumentNullException(nameof(item)));

    /// <summary>Insert the specified parameter into the collection.</summary>
    /// <param name="index">Index of the existing parameter before which to insert the new one.</param>
    /// <param name="value">Parameter to insert.</param>
    public void Insert(int index, SlonParameter value)
    {
        if ((uint)index > Count)
            throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than Count.");

        if (value is null)
            throw new ArgumentNullException(nameof(value));

        InsertCore(index, value.ParameterName, value);
    }

    /// <summary>Remove the specified parameter from the collection.</summary>
    /// <param name="value">Parameter to remove.</param>
    /// <returns>True if the parameter was found and removed, otherwise false.</returns>
    public bool Remove(SlonParameter value)
    {
        var index = IndexOfCore(value ?? throw new ArgumentNullException(nameof(value)));
        if (index == -1)
            return false;

        RemoveAtCore(index);
        return true;
    }

    /// <summary>Report the offset within the collection of the given parameter.</summary>
    /// <param name="value">Parameter to find.</param>
    /// <returns>Index of the parameter, or -1 if the parameter is not present.</returns>
    public int IndexOf(SlonParameter value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        return IndexOfCore(value);
    }

    /// <summary>Report whether the specified parameter is present in the collection.</summary>
    /// <param name="value">Parameter to find.</param>
    /// <returns>True if the parameter was found, otherwise false.</returns>
    public bool Contains(SlonParameter value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        return IndexOfCore(value) != -1;
    }

    /// <summary>Convert collection to a System.Array.</summary>
    /// <param name="array">Destination array.</param>
    /// <param name="arrayIndex">Starting index in destination array.</param>
    public void CopyTo(SlonParameter[] array, int arrayIndex) => CopyTo((Array)array, arrayIndex);

    /// <inheritdoc />
    bool ICollection<SlonParameter>.IsReadOnly => false;

    bool CanParameterBePositional => true;
    bool AlwaysCreateParameter => false;
    SlonParameter CreateParameter(string parameterName, object? value) => new(parameterName, value);
}
