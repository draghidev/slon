using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// Implementation
public sealed partial class SlonParameters
{
    const int LookupThreshold = 5;
    internal const string PositionalName = "";

    readonly struct ParameterItem
    {
        readonly string _name;
        readonly object? _value;
        readonly ParameterTypeResolution _typeResolution;
        readonly int _typeRevision;
        readonly bool _resolvedForPreparedType;

        ParameterItem(string name, object? value, ParameterTypeResolution typeResolution = default,
            int typeRevision = 0, bool resolvedForPreparedType = false)
        {
            if (value is DbParameter)
            {
                if (value is not SlonParameter p)
                    throw new InvalidCastException(
                        $"The DbParameter \"{value}\" is not of type \"{nameof(SlonParameter)}\" and cannot be used in this parameter collection, it can be added as a value to an {nameof(SlonParameter)} if this was intended.");

                // Prevent any changes from now on as the name may end up being used in the lookup.
                // We don't want the lookup to get out of sync but we also don't want any backreferences from parameter to collection so we freeze the name instead.
                p.NotifyCollectionAdd();

                if (!name.AsSpan().Equals(CreateNameSpan(p.ParameterName), StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException($"Parameter name must be a case-insensitive match with the property '{nameof(SlonParameter.ParameterName)}' on the given {nameof(SlonParameter)}.", nameof(name));
            }

            _name = name;
            _value = value;
            _typeResolution = typeResolution;
            _typeRevision = typeRevision;
            _resolvedForPreparedType = resolvedForPreparedType;
        }

        /// The canonical name used for uniqueness.
        public string Name => _name;

        /// Either null, an object or a SlonParameter, any other derived DbParameter types are not accepted.
        public object? Value => _value;
        internal ParameterTypeResolution TypeResolution => _typeResolution;

        public bool TryGetAsParameter([NotNullWhen(true)]out SlonParameter? parameter)
        {
            if (Value is SlonParameter p)
            {
                parameter = p;
                return true;
            }

            parameter = default;
            return false;
        }

        public KeyValuePair<string, object?> AsKeyValuePair() => new(_name, _value);

        public bool TryGetTypeResolution(PgSerializerOptions options, PgTypeId? preparedTypeId,
            out ParameterTypeResolution resolution)
        {
            resolution = _typeResolution;
            if (!resolution.IsResolved)
                return false;
            if (Value is SlonParameter parameter && parameter.TypeRevision != _typeRevision)
                return false;
            if (preparedTypeId is null)
                return !_resolvedForPreparedType;

            if (resolution.PgTypeId == preparedTypeId.Value)
                return true;
            return options.GetCanonicalTypeId(resolution.PgTypeId)
                == options.GetCanonicalTypeId(preparedTypeId.Value);
        }

        public ParameterItem WithTypeResolution(ParameterTypeResolution resolution, bool resolvedForPreparedType)
            => new(_name, _value, resolution,
                Value is SlonParameter parameter ? parameter.TypeRevision : 0,
                resolvedForPreparedType);

        public ParameterItem WithoutTypeResolution()
            => _typeResolution.IsResolved ? new(_name, _value) : this;

        public ParameterItem PreserveTypeResolutionFrom(in ParameterItem previous)
        {
            if (Value is SlonParameter || previous.Value is SlonParameter
                || Value?.GetType() != previous.Value?.GetType())
            {
                return this;
            }

            return new(_name, _value, previous._typeResolution, previous._typeRevision,
                previous._resolvedForPreparedType);
        }

        static int ComputePrefixLength(string name) => name.Length > 0 && name[0] is '@' or ':' ? 1 : 0;
        static string CreateName(string parameterName) => parameterName.Substring(ComputePrefixLength(parameterName));
        public static ReadOnlySpan<char> CreateNameSpan(string parameterName) => parameterName.AsSpan(ComputePrefixLength(parameterName));

        public static ParameterItem Create(string? parameterName, object? value)
        {
            if (parameterName is null)
            {
                // We allow all parameter types here and only fail in the constructor to give a nicer validation ordering.
                // This is a fallback for the T?/object? value accepting apis.
                if (value is not IDbDataParameter parameter)
                    throw new ArgumentNullException(nameof(parameterName));

                parameterName = parameter.ParameterName;
            }

            return new(CreateName(parameterName), value);
        }
    }

    readonly List<ParameterItem> _parameters;
    PgSerializerOptions? _parameterResolutionOptions;

    // Dictionary lookups for GetValue to improve performance.
    Dictionary<string, int>? _caseInsensitiveLookup;

    bool LookupEnabled => _parameters.Count >= LookupThreshold;

    internal ParameterTypeResolution GetOrResolveTypeInfo(int index, PgSerializerOptions options,
        PgTypeId? preparedTypeId, bool allowUnspecified)
    {
        ref var item = ref GetItemRef(index);
        var value = item.Value;

        if (allowUnspecified && value is (null or DBNull))
            return AdoParameterTypeResolver.Resolve(
                value, options, preparedTypeId, allowUnspecified);

        EnsureParameterResolutionOptions(options);
        if (item.TryGetTypeResolution(options, preparedTypeId, out var resolution))
            return resolution;

        resolution = AdoParameterTypeResolver.Resolve(
            value, options, preparedTypeId, allowUnspecified);
        item = item.WithTypeResolution(resolution,
            resolvedForPreparedType: preparedTypeId is not null);
        return resolution;
    }

    internal PgTypeId GetResolvedParameterType(int index)
        => GetItemRef(index).TypeResolution.PgTypeId;

    internal void GetResolvedParameter(int index, out object? value, out PgTypeInfo typeInfo)
    {
        ref var item = ref GetItemRef(index);
        value = item.Value;
        typeInfo = item.TypeResolution.GetTypeInfo(index);
    }

    void EnsureParameterResolutionOptions(PgSerializerOptions options)
    {
        if (ReferenceEquals(_parameterResolutionOptions, options))
            return;

        _parameterResolutionOptions = options;
        var parameters = CollectionsMarshal.AsSpan(_parameters);
        for (var i = 0; i < parameters.Length; i++)
            parameters[i] = parameters[i].WithoutTypeResolution();
    }

    static bool NameIsPositional(string name) => name is PositionalName;

    void LookupClear() => _caseInsensitiveLookup = null;

    void LookupAdd(string name, int index)
    {
        if (NameIsPositional(name))
            return;

        _caseInsensitiveLookup?.TryAdd(name, index);
    }

    void LookupInsert(string name, int index)
    {
        if (_caseInsensitiveLookup is null)
            return;

        foreach (var key in _caseInsensitiveLookup.Keys)
        {
            ref var mappedIndex = ref CollectionsMarshal.GetValueRefOrNullRef(_caseInsensitiveLookup, key);
            if (mappedIndex >= index)
                mappedIndex++;
        }

        if (!NameIsPositional(name)
            && (!_caseInsensitiveLookup.TryGetValue(name, out var existingIndex) || index < existingIndex))
        {
            _caseInsensitiveLookup[name] = index;
        }
    }

    void LookupRemove(string name, int index)
    {
        if (_caseInsensitiveLookup is null)
            return;

        var removedFirst = !NameIsPositional(name)
            && _caseInsensitiveLookup.TryGetValue(name, out var mappedIndex)
            && mappedIndex == index;
        if (removedFirst)
            _caseInsensitiveLookup.Remove(name);

        foreach (var key in _caseInsensitiveLookup.Keys)
        {
            ref var shiftedIndex = ref CollectionsMarshal.GetValueRefOrNullRef(_caseInsensitiveLookup, key);
            if (shiftedIndex > index)
                shiftedIndex--;
        }

        if (removedFirst)
        {
            for (var i = index; i < _parameters.Count; i++)
            {
                if (_parameters[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    _caseInsensitiveLookup[name] = i;
                    break;
                }
            }
        }
    }

    void LookupChangeName(ParameterItem item, string oldName, int index)
    {
        if (_caseInsensitiveLookup is null
            || oldName.Equals(item.Name, StringComparison.OrdinalIgnoreCase))
            return;

        if (!NameIsPositional(oldName)
            && _caseInsensitiveLookup.TryGetValue(oldName, out var oldIndex)
            && oldIndex == index)
        {
            _caseInsensitiveLookup.Remove(oldName);
            for (var i = index + 1; i < _parameters.Count; i++)
            {
                if (_parameters[i].Name.Equals(oldName, StringComparison.OrdinalIgnoreCase))
                {
                    _caseInsensitiveLookup[oldName] = i;
                    break;
                }
            }
        }

        if (!NameIsPositional(item.Name)
            && (!_caseInsensitiveLookup.TryGetValue(item.Name, out var newIndex) || index < newIndex))
        {
            _caseInsensitiveLookup[item.Name] = index;
        }
    }

    object? GetValue(int index) => _parameters[index].Value;

    ref ParameterItem GetItemRef(int index) => ref CollectionsMarshal.AsSpan(_parameters)[index];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    SlonParameter GetOrAddParameterInstance(int index)
    {
        ref var p = ref GetItemRef(index);
        if (p.TryGetAsParameter(out var parameter))
            return parameter;

        return ReplaceValue(ref p);

        SlonParameter ReplaceValue(ref ParameterItem p)
        {
            var parameter = CreateParameter(p.Name, p.Value);
            p = ParameterItem.Create(p.Name, parameter);
            return parameter;
        }
    }

    // parameterName may be null for a SlonParameter, whose own name is then used.
    int AddCore(string? parameterName, object? value)
    {
        var item = CreateItem(parameterName, value);
        _parameters.Add(item);
        LookupAdd(item.Name, _parameters.Count - 1);
        return _parameters.Count - 1;
    }

    ParameterItem CreateItem(string? parameterName, object? value)
    {
        if (parameterName is null && value is not SlonParameter)
            parameterName = PositionalName;

        return ParameterItem.Create(parameterName, value);
    }

    void ReplaceCore(int index, string? parameterName, object? value)
    {
        ref var current = ref GetItemRef(index);
        var item = CreateItem(parameterName, value).PreserveTypeResolutionFrom(current);
        var oldName = current.Name;
        current = item;
        LookupChangeName(item, oldName, index);
    }

    void InsertCore(int index, string? parameterName, object? value)
    {
        var item = CreateItem(parameterName, value);
        _parameters.Insert(index, item);
        // Positional parameters still shift the indices of named parameters after them.
        LookupInsert(item.Name, index);
    }

    void RemoveAtCore(int index)
    {
        var item = _parameters[index];
        _parameters.RemoveAt(index);
        if (_parameters.Count is 0)
            _parameterResolutionOptions = null;
        if (!LookupEnabled)
            LookupClear();
        else
            LookupRemove(item.Name, index);
    }

    int IndexOfCore(KeyValuePair<string, object?> item)
    {
        var index = IndexOfCore(item.Key);
        if (index == -1)
            return -1;

        var p = _parameters[index];
        if (Equals(item.Value, p.Value))
            return index;

        var name = ParameterItem.CreateNameSpan(item.Key);
        for (var i = index; i < _parameters.Count; i++)
        {
            p = _parameters[i];
            if (name.Equals(p.Name.AsSpan(), StringComparison.OrdinalIgnoreCase) && Equals(item.Value, p.Value))
                return i;
        }

        return -1;
    }

    int IndexOfCore(object? value)
    {
        for (var i = 0; i < _parameters.Count; i++)
        {
            var p = _parameters[i];
            if (Equals(value, p.Value))
                return i;
        }

        return -1;
    }

    int IndexOfCore(string parameterName)
    {
        var name = ParameterItem.CreateNameSpan(parameterName);

        // Using a dictionary is always faster after around 10 items when matched against reference equality.
        // For string equality this is the case after ~3 items so we take a decent compromise going with 5.
        if (LookupEnabled && name.Length != 0)
        {
            if (_caseInsensitiveLookup is null)
                BuildLookup();

            return _caseInsensitiveLookup!.GetValueOrDefault(name.ToString(), -1);
        }

        // Do case-insensitive search.
        for (var i = 0; i < _parameters.Count; i++)
        {
            var otherName = _parameters[i].Name;
            if (name.Equals(otherName.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;

        void BuildLookup()
        {
            _caseInsensitiveLookup = new(_parameters.Count, StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < _parameters.Count; i++)
            {
                var item = _parameters[i];
                if (!NameIsPositional(item.Name))
                    LookupAdd(item.Name, i);
            }
        }
    }

    bool TryGetValueCore(string parameterName, out object? value)
    {
        var index = IndexOfCore(parameterName);

        if (index == -1)
        {
            value = default;
            return false;
        }

        var p = _parameters[index];
        value = p.Value;
        return true;
    }

    internal struct NameValueEnumerator : IEnumerator<KeyValuePair<string, object?>>
    {
        readonly List<ParameterItem> _parameters;
        int _index;
        KeyValuePair<string, object?> _current;

        internal NameValueEnumerator(SlonParameters parameters) => _parameters = parameters._parameters;

        public NameValueEnumerator GetEnumerator()
        {
            var enumerator = this;
            enumerator.Reset();
            return enumerator;
        }

        public bool MoveNext()
        {
            var parameters = _parameters;

            if ((uint)_index < (uint)parameters.Count)
            {
                _current = parameters[_index].AsKeyValuePair();
                _index++;
                return true;
            }

            _current = default;
            _index = parameters.Count + 1;
            return false;
        }

        public KeyValuePair<string, object?> Current => _current;

        public void Reset()
        {
            _index = 0;
            _current = default;
        }

        object IEnumerator.Current => Current;

        public void Dispose() { }
    }

    internal NameValueEnumerator GetStructEnumerator() => new(this);

    SlonParameter CreateParameter(string parameterName, object? value) => new(parameterName, value);
}

// Public surface and ADO.NET
/// <summary>
/// Represents the parameters of a <see cref="SlonCommand"/> or <see cref="SlonBatchCommand"/>.
/// </summary>
public sealed partial class SlonParameters : DbParameterCollection,
    ICollection<KeyValuePair<string, object?>>, IDataParameterCollection, IList<SlonParameter>
{
    /// Initializes an empty parameter collection.
    public SlonParameters() : this(initialCapacity: 5) {}

    /// <summary>Initializes a parameter collection with the specified initial capacity.</summary>
    public SlonParameters(int initialCapacity)
    {
        _parameters = new(initialCapacity);
    }

    /// <summary>Initializes a parameter collection from parameter names and values.</summary>
    /// <param name="parameters">The parameters to add.</param>
    public SlonParameters(IEnumerable<KeyValuePair<string, object?>> parameters) : this(initialCapacity: 5)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        foreach (var (key, value) in parameters)
            Add(key, value);
    }

    /// <summary>Adds a parameter value with the given name.</summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    public void Add(string parameterName, object? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        AddCore(parameterName, value);
    }

    /// <summary>Adds a parameter value with the given name and DbType.</summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">The DbType for the parameter.</param>
    /// <param name="value">The value for the parameter.</param>
    public void Add(string parameterName, DbType dbType, object? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        var parameter = CreateParameter(parameterName, value);
        parameter.DbType = dbType;
        AddCore(parameterName, parameter);
    }

    /// <summary>Adds a parameter instance.</summary>
    /// <param name="parameter">The parameter instance.</param>
    public void Add(SlonParameter parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);
        AddCore(null, parameter);
    }

    /// <summary>Gets a value indicating whether a parameter with the specified name exists in the collection.</summary>
    /// <param name="parameterName">The name of the parameter to find.</param>
    /// <param name="value">
    /// A reference to the requested value, which can be  <see langword="null"/>, is returned if it is found in the list.
    /// This value is always <see langword="null"/> if the parameter is not found.
    /// </param>
    /// <returns>
    /// <see langword="true"/> whether the collection contains the parameter.
    /// Otherwise, <see langword="false"/>.
    /// </returns>
    public bool TryGetValue(string parameterName, out object? value)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        return TryGetValueCore(parameterName, out value);
    }

    /// <summary>Removes the parameter specified by the parameterName from the collection.</summary>
    /// <param name="parameterName">The name of the parameter to remove from the collection.</param>
    public void Remove(string parameterName)
    {
        ArgumentNullException.ThrowIfNull(parameterName);

        var index = IndexOfCore(parameterName);
        if (index == -1)
            throw new ArgumentException("A parameter with the given name was not found.", nameof(parameterName));

        RemoveAtCore(index);
    }

    /// <inheritdoc />
    IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator()
        => new NameValueEnumerator(this);

    /// <inheritdoc />
    void ICollection<KeyValuePair<string, object?>>.Add(KeyValuePair<string, object?> item)
    {
        if (item.Key is null)
            throw new ArgumentNullException(nameof(item), "Key is null.");

        AddCore(item.Key, item.Value);
    }

    /// <inheritdoc />
    bool ICollection<KeyValuePair<string, object?>>.Remove(KeyValuePair<string, object?> item)
    {
        if (item.Key is null)
            throw new ArgumentNullException(nameof(item), "Key is null.");

        var index = IndexOfCore(item);
        if (index == -1)
            return false;

        RemoveAtCore(index);
        return true;
    }

    /// <inheritdoc />
    bool ICollection<KeyValuePair<string, object?>>.Contains(KeyValuePair<string, object?> item)
    {
        if (item.Key is null)
            throw new ArgumentNullException(nameof(item), "Key is null.");

        return IndexOfCore(item) != -1;
    }

    /// <summary>Returns the names and values as they are stored, this means objects don't have to be non-null or of type <see cref="System.Data.Common.DbParameter"/>.</summary>
    /// <param name="array"></param>
    /// <param name="arrayIndex"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    void ICollection<KeyValuePair<string, object?>>.CopyTo(KeyValuePair<string, object?>[] array, int arrayIndex)
    {
        if (array is null)
            throw new ArgumentNullException(nameof(array));

        if ((uint)arrayIndex > array.Length)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex), "Index cannot be negative or larger than the length of the array.");

        if (arrayIndex + _parameters.Count > array.Length)
            throw new ArgumentOutOfRangeException(nameof(array), "Array too small.");

        for (var i = 0; i < _parameters.Count; i++)
        {
            var p = _parameters[i];
            array[arrayIndex + i] = new KeyValuePair<string, object?>(p.Name, p.Value);
        }
    }

    // Reimplemented IDataParameterCollection methods that otherwise do a cast in the setter to DbParameter on the object value.
    /// <inheritdoc cref="IList.this[int]" />
    object? IList.this[int index]
    {
        get
        {
            if ((uint)index >= _parameters.Count)
                throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than or equal to Count.");

            return GetValue(index);
        }
        set
        {
            if ((uint)index >= _parameters.Count)
                throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than or equal to Count.");

            ArgumentNullException.ThrowIfNull(value);
            ReplaceCore(index, null, value);
        }
    }

    /// <inheritdoc cref="IDataParameterCollection.this[string]" />
    object IDataParameterCollection.this[string parameterName]
    {
        get
        {
            if (parameterName is null)
                throw new ArgumentNullException(nameof(parameterName));

            var index = IndexOfCore(parameterName);
            if (index == -1)
                throw new ArgumentException("A parameter with the given name was not found.", nameof(parameterName));

            return GetOrAddParameterInstance(index);
        }
        set
        {
            if (parameterName is null)
                throw new ArgumentNullException(nameof(parameterName));
            ArgumentNullException.ThrowIfNull(value);

            var index = IndexOfCore(parameterName);
            if (index == -1)
                AddCore(parameterName, value);
            else
                ReplaceCore(index, parameterName, value);
        }
    }

    /// <inheritdoc cref="IList.Add" />
    int IList.Add(object? value) => AddCore(null, value);

    /// <inheritdoc cref="DbParameterCollection.GetEnumerator" />
    public override IEnumerator GetEnumerator()
    {
        for (var i = 0; i < Count; i++)
            yield return GetOrAddParameterInstance(i);
    }

    /// <inheritdoc cref="DbParameterCollection.Add" />
    public override int Add(object value) => AddCore(null, value ?? throw new ArgumentNullException(nameof(value)));

    /// <inheritdoc cref="DbParameterCollection.AddRange" />
    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);

        foreach (var parameter in values)
            AddCore(null, parameter);
    }

    /// <inheritdoc cref="DbParameterCollection.Insert" />
    public override void Insert(int index, object? value)
    {
        if ((uint)index > _parameters.Count)
            throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than Count.");

        InsertCore(index, null, value ?? throw new ArgumentNullException(nameof(value)));
    }

    /// <inheritdoc cref="DbParameterCollection.RemoveAt(string)" />
    public override void RemoveAt(string parameterName) => RemoveAtCore(IndexOfCore(parameterName ?? throw new ArgumentNullException(nameof(parameterName))));

    /// <inheritdoc cref="DbParameterCollection.RemoveAt(int)" />
    public override void RemoveAt(int index)
    {
        if ((uint)index >= _parameters.Count)
            throw new ArgumentOutOfRangeException(nameof(index), "Index cannot be negative or larger than or equal to Count.");

        RemoveAtCore(index);
    }

    /// <inheritdoc cref="DbParameterCollection.Remove" />
    public override void Remove(object? value) => RemoveAtCore(IndexOfCore(value ?? throw new ArgumentNullException(nameof(value))));

    /// <inheritdoc cref="DbParameterCollection.IndexOf(string)" />
    public override int IndexOf(string? parameterName) => parameterName is null ? -1 : IndexOfCore(parameterName);

    /// <inheritdoc cref="DbParameterCollection.IndexOf(object)" />
    public override int IndexOf(object? value) => value is null ? -1 : IndexOfCore(value);

    /// <inheritdoc cref="DbParameterCollection.Contains(string)" />
    public override bool Contains(string? parameterName) => parameterName is not null && IndexOfCore(parameterName) != -1;

    /// <inheritdoc cref="DbParameterCollection.Contains(object)" />
    public override bool Contains(object? value) => value is not null && IndexOfCore(value) != -1;

    /// <inheritdoc cref="DbParameterCollection.CopyTo" />
    public override void CopyTo(Array array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);

        if ((uint)arrayIndex > array.Length)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex), "Index cannot be negative or larger than the length of the array.");

        if (arrayIndex + _parameters.Count > array.Length)
            throw new ArgumentOutOfRangeException(nameof(array), "Array too small.");

        var list = array as IList;
        for (var i = 0; i < _parameters.Count; i++)
            list[arrayIndex + i] = GetOrAddParameterInstance(i);
    }

    /// <inheritdoc cref="DbParameterCollection.Count" />
    public override int Count => _parameters.Count;

    /// <inheritdoc cref="DbParameterCollection.IsReadOnly" />
    public override bool IsReadOnly => false;

    /// <inheritdoc cref="DbParameterCollection.IsFixedSize" />
    public override bool IsFixedSize => false;

    /// <inheritdoc cref="DbParameterCollection.IsSynchronized" />
    public override bool IsSynchronized => false;

    /// <inheritdoc cref="DbParameterCollection.SyncRoot" />
    public override object SyncRoot => _parameters;

    /// <inheritdoc cref="DbParameterCollection.Clear" />
    public override void Clear()
    {
        LookupClear();
        _parameters.Clear();
        _parameterResolutionOptions = null;
    }

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => (DbParameter)((IList)this)[index]!;
    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName) => (DbParameter)((IDataParameterCollection)this)[parameterName];
    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value) => ((IList)this)[index] = value;
    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value) => ((IDataParameterCollection)this)[parameterName] = value;

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
        ArgumentNullException.ThrowIfNull(parameters);

        foreach (var (key, value) in parameters)
            Add(key, value);
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

        var parameter = new SlonParameter<T>(parameterName, value)
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

}
