using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// Implementation
public partial class SlonParameters
{
    // Internal for tests
    internal const int LookupThreshold = 5;
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

    /// <summary>
    /// Initializes a new instance of the DbDataParameterCollection class.
    /// </summary>
    public SlonParameters(int initialCapacity = 5)
    {
        _parameters = new(initialCapacity);
    }

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    string GetName(int index) => _parameters[index].Name;

    bool NameIsPositional(string name) => CanParameterBePositional && name is PositionalName;

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
                if (GetName(i).Equals(name, StringComparison.OrdinalIgnoreCase))
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
                if (GetName(i).Equals(oldName, StringComparison.OrdinalIgnoreCase))
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

    int AddCore(DbParameter parameter)
    {
        var item = ParameterItem.Create(null, parameter);
        _parameters.Add(item);
        LookupAdd(item.Name, _parameters.Count - 1);
        return _parameters.Count - 1;
    }

    /// parameterName can only be null if object is an instance of SlonParameter.
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
        {
            if (CanParameterBePositional)
                parameterName = PositionalName;
            else if (!AlwaysCreateParameter)
                ThrowHelper.ThrowInvalidOperation($"Parameters must be of type {nameof(SlonParameter)} when no parameter name is given.");
        }

        if (AlwaysCreateParameter && value is not SlonParameter)
        {
            Debug.Assert(parameterName is not null);
            value = CreateParameter(parameterName, value);
        }

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
        // Also called if the item is positional, the lookup needs to be shifted to account for the insert.
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
            var otherName = GetName(i);
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

    struct NameValueEnumerator : IEnumerator<KeyValuePair<string, object?>>
    {
        readonly List<ParameterItem> _parameters;
        int _index;
        KeyValuePair<string, object?> _current;

        internal NameValueEnumerator(SlonParameters parameters) => _parameters = parameters._parameters;

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

    // Beautiful ADO.NET 1.0 design to fill the public GetEnumerator method slot with a non generic IEnumerable method...
    NameValueEnumerator GetNameValueEnumerator() => new(this);
}

// Public surface & ADO.NET
/// <inheritdoc cref="System.Data.Common.DbParameterCollection"/>
public partial class SlonParameters : DbParameterCollection, ICollection<KeyValuePair<string, object?>>, IDataParameterCollection
{
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
        AddCore(parameter);
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
    IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => GetNameValueEnumerator();

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
}
