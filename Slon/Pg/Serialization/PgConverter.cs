using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Slon.Pg.Serialization.Converters.IEnumUnderlyingConverter;

namespace Slon.Pg.Serialization;

public abstract class PgConverter
{
    internal virtual bool RequiresReaderCleanup => true;
    internal bool IsReadViewBased { get; init; }
    internal virtual bool ResultIsColumnLease => false;

    /// <summary>
    /// True when CLR null can reach this converter's API surface.
    /// Auto-derived from <see cref="TypeToConvert"/> (or from an internal wrapper's effective type if passed).
    /// Orthogonal to <see cref="HandleDbNull"/>: the two combine into <see cref="DbNullPredicateKind"/> as Custom (HandleDbNull true),
    /// Null (HandleDbNull false, TypeAcceptsNull true), or None (both false).
    /// </summary>
    internal bool TypeAcceptsNull { get; }
    internal DbNullPredicate DbNullPredicateKind
        => HandleDbNull ? DbNullPredicate.Custom
            : TypeAcceptsNull ? DbNullPredicate.Null
            : DbNullPredicate.None;
    public bool IsDbNullable => DbNullPredicateKind is not DbNullPredicate.None;

    private protected PgConverter(Type type, bool typeAcceptsNull)
    {
        TypeToConvert = type;
        TypeAcceptsNull = typeAcceptsNull;
    }

    /// <summary>
    /// True when the converter has a custom IsDbNullValue override that should be consulted to determine db-nullness.
    /// When false, db-nullness is decided purely based on whether the <see cref="TypeToConvert"/> accepts nulls naturally.
    /// </summary>
    protected internal bool HandleDbNull { get; init; }

    /// <summary>
    /// Computes the converter's descriptor under the supplied descriptor context.
    /// The framework calls this in the context of the format this converter is registered for.
    /// The returned descriptor describes the converter's attributes for that context.
    /// Encoding dependent text-format converters can read <see cref="PgConversionContext.TextEncoding"/> via
    /// <see cref="DescriptorContext.ConversionContext"/>.
    /// </summary>
    /// <remarks>
    /// The default implementation returns <see cref="BufferRequirements.Streaming"/>.
    /// Override to declare a tighter shape (fixed-size, upper-bound, invariant).
    /// </remarks>
    public virtual ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => new() { BufferRequirements = BufferRequirements.Streaming };

    internal Type TypeToConvert { get; }

    // Dispatch helpers below all gate on `typeof(T) == TypeToConvert` rather than `this is PgConverter<T>`:
    // a Type-handle reference compare avoids the isinst MethodTable chain walk per call. Both produce the
    // same answer but typeof equality is cheaper on the hot path.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    PgConverter<T> UnsafeAs<T>()
    {
        // Justification: avoid perf cost of casting to a known base class type per dispatch call.
        Debug.Assert(typeof(T) == TypeToConvert);
        Debug.Assert(this is PgConverter<T>);
        return Unsafe.As<PgConverter<T>>(this);
    }

    /// <summary>Reads a value from the reader as <typeparamref name="T"/>.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public
#nullable disable // T may or may not be nullable depending on the converter's read behavior.
    T
#nullable restore
    Read<T>(PgReader reader)
        => typeof(T) == TypeToConvert
            ? UnsafeAs<T>().Read(reader)
            : IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported
                ? ReadAsEnumUnderlying<T>(reader)
                : (T)ReadAsObject(reader)!;

    /// <summary>Asynchronously reads a value from the reader as <typeparamref name="T"/>.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<
#nullable disable // T may or may not be nullable depending on the converter's read behavior.
    T
#nullable restore
    > ReadAsync<T>(PgReader reader, CancellationToken cancellationToken = default)
    {
        if (typeof(T) == TypeToConvert)
            return UnsafeAs<T>().ReadAsync(reader, cancellationToken);

        if (IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported)
            return new(ReadAsEnumUnderlying<T>(reader));

        var task = ReadAsObjectAsync(reader, cancellationToken);
        return task.IsCompletedSuccessfully ? new((T)task.Result!) : ReadAndUnboxAsync(task);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static async ValueTask<T> ReadAndUnboxAsync(ValueTask<object?> task)
            => (T)(await task.ConfigureAwait(false))!;
    }

    /// <summary>Db-null check for <typeparamref name="T"/>.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsDbNull<T>(T? value, object? writeState)
    {
        if (typeof(T) == TypeToConvert)
            return UnsafeAs<T>().IsDbNull(value, writeState);

        if (IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported)
            return IsDbNullAsEnumUnderlying(value, writeState);

        return IsDbNullAsObject(value, writeState);
    }

    /// <summary>Computes the serialized size for <paramref name="value"/>, producing any required <paramref name="writeState"/>.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Size Bind<T>(in BindContext context, T value, ref object? writeState)
    {
        if (typeof(T) == TypeToConvert)
            return UnsafeAs<T>().Bind(context, value, ref writeState);

        if (IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported)
            return BindAsEnumUnderlying(context, value, ref writeState);

        return BindAsObject(context, value, ref writeState);
    }

    /// <summary>Writes a <typeparamref name="T"/> value to the writer.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<T>(PgWriter writer, T value)
    {
        if (typeof(T) == TypeToConvert)
        {
            UnsafeAs<T>().Write(writer, value);
            return;
        }

        if (IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported)
        {
            WriteAsEnumUnderlying(writer, value);
            return;
        }

        WriteAsObject(writer, value);
    }

    /// <summary>Asynchronously writes a <typeparamref name="T"/> value to the writer.</summary>
    /// <remarks>Dispatches to the typed converter when <typeparamref name="T"/> matches <see cref="TypeToConvert"/>; otherwise routes through the object-erased path.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask WriteAsync<T>(PgWriter writer, T value, CancellationToken cancellationToken = default)
    {
        if (typeof(T) == TypeToConvert)
            return UnsafeAs<T>().WriteAsync(writer, value, cancellationToken);

        if (IsEnumUnderlyingConversion<T>(this) && RuntimeFeature.IsDynamicCodeSupported)
        {
            WriteAsEnumUnderlying(writer, value);
            return new();
        }

        return WriteAsObjectAsync(writer, value, cancellationToken);
    }

    /// Checks whether <paramref name="value"/> is considered a database null by this converter.
    public bool IsDbNullAsObject(object? value, object? writeState)
    {
        if (value is null && !TypeAcceptsNull)
            ThrowInvalidNullValue();
        return DbNullPredicateKind switch
        {
            DbNullPredicate.Null => value is null,
            DbNullPredicate.None => false,
            DbNullPredicate.Custom => IsDbNullValueAsObject(value, writeState),
            _ => ThrowDbNullPredicateOutOfRange()
        };
    }

    private protected abstract bool IsDbNullValueAsObject(object? value, object? writeState);

    private protected abstract Size BindValueAsObject(in BindContext context, object? value, ref object? writeState);

    /// Computes the serialized size for <paramref name="value"/>, producing any required <paramref name="writeState"/>.
    public Size BindAsObject(in BindContext context, object? value, ref object? writeState)
    {
        if (value is null && !TypeAcceptsNull)
            ThrowInvalidNullValue();

        if (context.IsBindOptional)
        {
            if (context.BufferRequirement.Kind is not SizeKind.Exact)
                ThrowHelper.ThrowInvalidOperation(
                    $"{nameof(BufferRequirements.IsBindOptional)}=true requires an {nameof(SizeKind.Exact)} buffer requirement.");
            return context.BufferRequirement;
        }

        // writeState identity discipline:
        // - Fixed-size: any change is forbidden (no production, no swap, no clear).
        // - Non-fixed-size: monotonic — null → non-null is the production transition; an existing
        //   IDisposable identity must be preserved (provenance carries disposal obligation), and
        //   clearing is forbidden. Non-disposable references may be replaced by another non-null
        //   reference without violating the lifecycle.
        var originalWriteState = writeState;
        Size size;
        try
        {
            size = BindValueAsObject(context, value, ref writeState);

            if ((originalWriteState is not null || context.IsBindFixedSize) && !ReferenceEquals(originalWriteState, writeState)
                && (context.IsBindFixedSize || writeState is null || originalWriteState is IDisposable))
                ThrowWriteStateLifecycleViolation(context.IsBindFixedSize);

            // Catches non-Exact sizes from both paths (the IsBindFixedSize Kind check is folded in here —
            // a converter declaring fixed-size that returned a non-Exact size trips this same throw).
            switch (size.Kind)
            {
            case SizeKind.UpperBound:
                ThrowHelper.ThrowInvalidOperation($"{nameof(SizeKind.UpperBound)} is not a valid return value for BindValue.");
                break;
            case SizeKind.Unknown:
                ThrowHelper.ThrowInvalidOperation($"{nameof(SizeKind.Unknown)} is not a valid return value for BindValue.");
                break;
            }
        }
        catch
        {
            // Contract: writeState transitions to null on throw. BindValue is free to assign partial
            // state to writeState as it works (composing converters do this so their wrapper's Dispose
            // can clean up populated slots). The framework's safety net here disposes anything still
            // observable to us and nulls the slot, so callers see a uniform "clean on throw" semantic.
            // Both current and original may need disposing on a lifecycle-violation swap; the
            // ReferenceEquals gate collapses to a single Dispose on the legal no-swap case.
            // Null first, then dispose: a throwing Dispose must not leave callers with a non-null
            // writeState pointing at a half-disposed object — they'd dispose it again.
            (var current, writeState) = (writeState, null);
            (current as IDisposable)?.Dispose();
            // current==null with original!=null means an inner safety net (transparent wrapper case)
            // already disposed the original. Disposing again would double-dispose. The non-null branch
            // handles the genuine lifecycle-violation swap where current and original differ.
            if (current is not null && !ReferenceEquals(current, originalWriteState))
                (originalWriteState as IDisposable)?.Dispose();
            throw;
        }

        return size;
    }

    /// Reads a value from the reader.
    public object? ReadAsObject(PgReader reader)
        => ReadAsObject(async: false, reader, CancellationToken.None).Result;
    /// Asynchronously reads a value from the reader.
    public ValueTask<object?> ReadAsObjectAsync(PgReader reader, CancellationToken cancellationToken = default)
        => ReadAsObject(async: true, reader, cancellationToken);

    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    internal abstract ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken);

    /// Writes <paramref name="value"/> to the writer.
    public void WriteAsObject(PgWriter writer, object? value)
        => WriteAsObject(async: false, writer, value, CancellationToken.None).GetAwaiter().GetResult();
    /// Asynchronously writes <paramref name="value"/> to the writer.
    public ValueTask WriteAsObjectAsync(PgWriter writer, object? value, CancellationToken cancellationToken = default)
        => WriteAsObject(async: true, writer, value, cancellationToken);

    // Shared sync/async abstract to reduce virtual method table size overhead and code size for each NpgsqlConverter<T> instantiation.
    internal abstract ValueTask WriteAsObject(bool async, PgWriter writer, object? value, CancellationToken cancellationToken);

    internal enum DbNullPredicate : byte
    {
        /// Never DbNull (struct types)
        None,
        /// DbNull when *user code*
        Custom,
        /// DbNull when value is null
        Null
    }

    [DoesNotReturn]
    private protected void ThrowIORequired(Size bufferRequirement)
        => throw new InvalidOperationException($"Buffer requirement '{bufferRequirement}' not respected for converter '{GetType().FullName}', expected no IO to be required.");

    private protected static bool ThrowInvalidNullValue()
        => throw new ArgumentNullException("value", "Null value given for non-nullable type converter");

    private protected bool ThrowDbNullPredicateOutOfRange()
        => throw new UnreachableException($"Unknown case {DbNullPredicateKind.ToString()}");

    [DoesNotReturn]
    private protected static void ThrowWriteStateLifecycleViolation(bool isBindFixedSize)
    {
        if (isBindFixedSize)
            throw new InvalidOperationException("Fixed-size BindValue must not modify the writeState reference.");
        throw new InvalidOperationException("BindValue must not orphan an IDisposable writeState reference, nor clear an existing one.");
    }
}

public abstract class PgConverter<T> : PgConverter
{
    private protected PgConverter() : base(typeof(T), default(T) is null) { }

    private protected PgConverter(Type effectiveType)
        : base(typeof(T), !effectiveType.IsValueType || Nullable.GetUnderlyingType(effectiveType) is not null) { }

    protected virtual bool IsDbNullValue(T? value, object? writeState)
        => throw new NotSupportedException(
            $"Converters with {nameof(HandleDbNull)} enabled must override {nameof(IsDbNullValue)}.");

    private protected override bool IsDbNullValueAsObject(object? value, object? writeState)
        => IsDbNullValue((T?)value, writeState);

    /// Checks whether <paramref name="value"/> is considered a database null by this converter.
    public bool IsDbNull(T? value, object? writeState)
    {
        Debug.Assert(value is not null || TypeAcceptsNull, "TypeAcceptsNull issue, null reached the typed IsDbNull on a converter whose T does not accept null.");
        return DbNullPredicateKind switch
        {
            DbNullPredicate.Null => value is null,
            DbNullPredicate.None => false,
            DbNullPredicate.Custom => IsDbNullValue(value, writeState),
            _ => ThrowDbNullPredicateOutOfRange()
        };
    }

    /// Reads a <typeparamref name="T"/> value from the reader.
    public abstract
#nullable disable // T may or may not be nullable depending on the derived converter's read behavior.
    T
#nullable restore
    Read(PgReader reader);

    /// Asynchronously reads a <typeparamref name="T"/> value from the reader.
    public abstract ValueTask<
#nullable disable // T may or may not be nullable depending on the derived converter's read behavior.
        T
#nullable restore
    > ReadAsync(PgReader reader, CancellationToken cancellationToken = default);

    /// Computes the serialized size for <paramref name="value"/>, producing any required <paramref name="writeState"/>.
    public Size Bind(in BindContext context,
#nullable disable // T may or may not be nullable depending on the derived converter's IsDbNullValue override.
        T value,
#nullable restore
        ref object? writeState)
    {
        Debug.Assert(TypeAcceptsNull || value is not null);

        if (context.IsBindOptional)
        {
            if (context.BufferRequirement.Kind is not SizeKind.Exact)
                ThrowHelper.ThrowInvalidOperation(
                    $"{nameof(BufferRequirements.IsBindOptional)}=true requires an {nameof(SizeKind.Exact)} buffer requirement.");
            return context.BufferRequirement;
        }

        // writeState identity discipline:
        // - Fixed-size: any change is forbidden (no production, no swap, no clear).
        // - Non-fixed-size: monotonic — null → non-null is the production transition; an existing
        //   IDisposable identity must be preserved (provenance carries disposal obligation), and
        //   clearing is forbidden. Non-disposable references may be replaced by another non-null
        //   reference (e.g. polymorphic-dispatch upgrade) without violating the lifecycle.
        var originalWriteState = writeState;
        Size size;
        try
        {
            size = BindValue(context, value, ref writeState);

            if ((originalWriteState is not null || context.IsBindFixedSize) && !ReferenceEquals(originalWriteState, writeState)
                && (context.IsBindFixedSize || writeState is null || originalWriteState is IDisposable))
                ThrowWriteStateLifecycleViolation(context.IsBindFixedSize);

            switch (size.Kind)
            {
            case SizeKind.UpperBound:
                ThrowHelper.ThrowInvalidOperation($"{nameof(SizeKind.UpperBound)} is not a valid return value for {nameof(BindValue)}.");
                break;
            case SizeKind.Unknown:
                ThrowHelper.ThrowInvalidOperation($"{nameof(SizeKind.Unknown)} is not a valid return value for {nameof(BindValue)}.");
                break;
            }
        }
        catch
        {
            // Contract: writeState transitions to null on throw. BindValue is free to assign partial
            // state to writeState as it works (composing converters do this so their wrapper's Dispose
            // can clean up populated slots). The framework's safety net here disposes anything still
            // observable to us and nulls the slot, so callers see a uniform "clean on throw" semantic.
            // Both current and original may need disposing on a lifecycle-violation swap; the
            // ReferenceEquals gate collapses to a single Dispose on the legal no-swap case.
            // Null first, then dispose: a throwing Dispose must not leave callers with a non-null
            // writeState pointing at a half-disposed object — they'd dispose it again.
            (var current, writeState) = (writeState, null);
            (current as IDisposable)?.Dispose();
            // current==null with original!=null means an inner safety net (transparent wrapper case)
            // already disposed the original. Disposing again would double-dispose. The non-null branch
            // handles the genuine lifecycle-violation swap where current and original differ.
            if (current is not null && !ReferenceEquals(current, originalWriteState))
                (originalWriteState as IDisposable)?.Dispose();
            throw;
        }

        return size;
    }

    /// <summary>Per-value bind step for <typeparamref name="T"/>. Computes the wire size and produces any
    /// <paramref name="writeState"/> needed by the subsequent write phase. <see cref="Bind"/> wraps this
    /// call and enforces size-kind invariants.</summary>
    protected virtual Size BindValue(in BindContext context,
#nullable disable // T may or may not be nullable depending on the derived converter's IsDbNullValue override.
        T value,
#nullable restore
        ref object? writeState)
        => throw new NotSupportedException($"Converter must override {nameof(BindValue)}.");

    /// Writes a <typeparamref name="T"/> value to the writer.
    public abstract void Write(PgWriter writer,
#nullable disable // T may or may not be nullable depending on the derived converter's IsDbNullValue override.
        T value
#nullable restore
        );

    /// Asynchronously writes a <typeparamref name="T"/> value to the writer.
    public abstract ValueTask WriteAsync(PgWriter writer,
#nullable disable // T may or may not be nullable depending on the derived converter's IsDbNullValue override.
        T value,
#nullable restore
        CancellationToken cancellationToken = default);

    private protected sealed override Size BindValueAsObject(in BindContext context, object? value, ref object? writeState)
        => BindValue(context, (T)value!, ref writeState);
}

static class PgConverterExtensions
{
    /// Checks whether <paramref name="value"/> is considered a database null under the given <paramref name="handling"/> policy.
    public static bool IsDbNullAsNestedObject(this PgConverter converter, object? value, object? writeState, NestedObjectDbNullHandling handling)
    {
        switch (handling)
        {
        case NestedObjectDbNullHandling.ExtendedThrowOnNull:
            if (value is null)
                throw new ArgumentNullException(nameof(value), "Object-typed value cannot be null; use a database-null value instead.");
            goto case NestedObjectDbNullHandling.Extended;
        case NestedObjectDbNullHandling.Extended:
            if (value is DBNull)
                return true;
            goto case NestedObjectDbNullHandling.Default;
        case NestedObjectDbNullHandling.Default:
            return value is null || converter.IsDbNullAsObject(value, writeState);
        default:
            throw new UnreachableException();
        }
    }
}

public readonly struct BindContext
{
    /// <summary>The data format selected for this bind.</summary>
    public DataFormat Format { get; private init; }

    /// <summary>
    /// The conversion context active for this bind. Forwarded through nested binds; populated from
    /// the writer at the outermost bind so composing converters can resolve context-dependent inner
    /// descriptors against the same context the eventual Write operation will see.
    /// </summary>
    public PgConversionContext ConversionContext { get; private init; }

    /// <summary>
    /// The size requirement for writing values with <see cref="Format"/>.
    /// Sourced from the format-specific <see cref="BufferRequirements.Write"/> returned by <see cref="PgConverter.GetDescriptor"/>.
    /// </summary>
    public Size BufferRequirement { get; private init; }

    /// <summary>
    /// When true, composing converters may use <see cref="BufferRequirement"/> directly and skip the nested <c>Bind</c> call entirely.
    /// <c>Bind</c> can be called anyway at which point it just short-circuits, without invoking <c>BindValue</c>. Implies <see cref="IsBindFixedSize"/>.
    /// Sourced from the format-specific <see cref="BufferRequirements.IsBindOptional"/> returned by <see cref="PgConverter.GetDescriptor"/>.
    /// </summary>
    public bool IsBindOptional { get; private init; }

    /// <summary>
    /// True when <see cref="BufferRequirement"/> is value-independent — every value's size equals
    /// <see cref="BufferRequirement"/>. Composing converters use this to compute closed-form aggregate
    /// sizes without per-element ledgers, even when <c>Bind</c> must still be called for side effects
    /// (validation, etc.). Synthesized from <see cref="Size.Kind"/> being <see cref="SizeKind.Exact"/>.
    /// </summary>
    public bool IsBindFixedSize => BufferRequirement.Kind is SizeKind.Exact;

    // Public init as this can be caller decided.
    /// <summary>
    /// The policy for how nested object-typed values should have their database null-shaped values handled during this bind.
    /// See <see cref="NestedObjectDbNullHandling"/> for per-mode semantics.
    /// </summary>
    public NestedObjectDbNullHandling NestedObjectDbNullHandling { get; init; }

    /// <summary>
    /// Constructs a <see cref="BindContext"/> from a converter info, propagating relevant context from <paramref name="nestingContext"/>.
    /// Composing converters (arrays, ranges, multiranges, composites, etc.) use this to thread any policy through to nested binds.
    /// </summary>
    public static BindContext CreateNested(in BindContext nestingContext, PgConverter converter)
    {
        var bufferRequirements = converter.GetDescriptor(
            new() { ConversionContext = nestingContext.ConversionContext }).BufferRequirements;
        return CreateNested(nestingContext, bufferRequirements);
    }

    /// <summary>
    /// Variant of <see cref="CreateNested(in BindContext, PgConverter)"/> for callers that already
    /// hold the inner converter's <see cref="BufferRequirements"/> (e.g. composing converters that
    /// captured them in their constructor). Skips the per-call <see cref="PgConverter.GetDescriptor"/> roundtrip.
    /// </summary>
    public static BindContext CreateNested(in BindContext nestingContext, BufferRequirements requirements)
        => new()
        {
            Format = nestingContext.Format,
            BufferRequirement = requirements.Write,
            IsBindOptional = requirements.IsBindOptional,
            NestedObjectDbNullHandling = nestingContext.NestedObjectDbNullHandling,
            ConversionContext = nestingContext.ConversionContext,
        };

    /// <summary>
    /// Constructs a <see cref="BindContext"/> from caller-supplied values without verifying that
    /// <paramref name="bufferRequirement"/> and <paramref name="isBindOptional"/> match the converter's
    /// cached requirements. Callers must ensure these values are consistent with the converter that
    /// will receive this context.
    /// </summary>
    public static BindContext CreateUnchecked(DataFormat format, Size bufferRequirement, bool isBindOptional, PgConversionContext? conversionContext = null)
        => new()
        {
            Format = format,
            BufferRequirement = bufferRequirement,
            IsBindOptional = isBindOptional,
            ConversionContext = conversionContext ?? PgConversionContext.Empty,
        };
}

/// <summary>
/// How null-shaped values are pre-filtered when a container's element or field slot is erased to <see cref="object"/>.
/// CLR semantics are the floor (<see cref="Default"/>), extended modes layer database null sentinel recognition on top.
/// Strongly-typed slots resolve nulls through the nested converter directly and don't consult this knob.
/// </summary>
/// <remarks>
/// Parameter-shaped containers (e.g. an <c>object[]</c> parameter) use <see cref="Extended"/> because the
/// parameter layer treats database null sentinels as a first-class null expression alongside CLR null.
/// Typed composites generally use <see cref="Default"/>. These create a new serialization scope where database null sentinels are not recognized.
/// </remarks>
public enum NestedObjectDbNullHandling
{
    /// <summary>CLR null becomes a database null. Database null sentinels are passed through to the nested converter.</summary>
    Default = 0,
    /// <summary>CLR null and database null sentinels both become a database null.</summary>
    Extended,
    /// <summary>CLR null throws. Database null sentinels become a database null.</summary>
    ExtendedThrowOnNull
}

class MultiWriteState : IDisposable
{
    public ArrayPool<(Size Size, object? WriteState)>? ArrayPool { get; set; }
    public ArraySegment<(Size Size, object? WriteState)> Data { get; set; }
    public bool AnyWriteState { get; set; }
    int _disposed;

    public void Dispose()
    {
        // Atomic idempotency guard — double-dispose returns the rented array to the pool twice, handing
        // the same buffer to two different renters. Atomic also catches concurrent disposal once states
        // start being reusable across executions (StableValue) where threading lifetimes broaden.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            Debug.Assert(false, "MultiWriteState double-dispose detected — caller violated lifecycle contract.");
            return;
        }

        if (Data.Array is not { } array)
            return;

        if (AnyWriteState)
        {
            for (var i = Data.Offset; i < Data.Offset + Data.Count; i++)
                if (array[i].WriteState is IDisposable disposable)
                    disposable.Dispose();

            Array.Clear(Data.Array, Data.Offset, Data.Count);
        }

        ArrayPool?.Return(Data.Array);
    }
}

/// <summary>
/// Connection/options-scoped state that flows into a converter across operations (descriptor query,
/// read, write, bind). Carries session state the converter may need (today: <see cref="TextEncoding"/>
/// for text-format converters; future: dynamic per-connection state). One instance is shared across all
/// callers within the same scope, so consumers must treat it as a read-through reference and avoid
/// per-call allocation.
/// </summary>
public sealed class PgConversionContext
{
    /// <summary>An empty context, suitable for inner probes that don't read any session state.</summary>
    public static PgConversionContext Empty { get; } = new();

    /// <summary>
    /// The text encoding for this context. Defaults to UTF-8 — the substrate's default and the only
    /// encoding for which no fallback semantics are needed. Always set; converters can read it
    /// unconditionally without null-checking.
    /// </summary>
    public Encoding TextEncoding { get; init; } = Encoding.UTF8;

    /// <summary>
    /// Connector-cached <see cref="Encoder"/> over <see cref="TextEncoding"/>, surfaced for framework-internal
    /// callers (PgWriter slow paths) that would otherwise allocate fresh encoders per call. Getter calls
    /// <see cref="Encoder.Reset"/> before returning so each caller observes clean state. Null when no
    /// connection is in scope (e.g. probes against <see cref="Empty"/>); callers fall back to a fresh
    /// <see cref="Encoding.GetEncoder"/> in that case.
    /// </summary>
    /// <remarks>
    /// Intentionally <c>internal</c>: sharing a stateful encoder across the public converter surface is a
    /// composition hazard when conversions can suspend mid-encode. Framework call sites acquire-and-finish
    /// within a single method (no nested converter dispatch between acquisition and final
    /// <c>Encoder.Convert</c>), so the reset-on-getter pattern is safe there.
    /// </remarks>
    internal Encoder? TextEncoder
    {
        get
        {
            if (field is null) return null;
            field.Reset();
            return field;
        }
        init;
    }

    /// <summary>
    /// The session's PostgreSQL TimeZone setting (IANA/Olson name), as last reported by the server's
    /// <c>ParameterStatus</c> stream. Null when no connection is in scope (e.g. probes against
    /// <see cref="Empty"/>) — converters that depend on it must throw a meaningful error in that case
    /// rather than fall back silently. The connector replaces its <see cref="PgConversionContext"/>
    /// instance when this changes, so converters can read it without staleness concerns.
    /// </summary>
    public string? TimeZone { get; init; }
}

/// <summary>
/// Per-call wrapper around a <see cref="PgConversionContext"/> that <see cref="PgConverter.GetDescriptor"/>
/// receives. Hosts call-scoped state that doesn't belong on the long-lived <see cref="PgConversionContext"/>
/// Consumers read <see cref="PgConversionContext"/> for session state.
/// </summary>
public readonly struct DescriptorContext
{
    public PgConversionContext ConversionContext { get; init; }
}

/// A converter's description of itself for a given <see cref="DescriptorContext"/> (or invariant).
public readonly struct ConverterDescriptor
{
    /// <summary>
    /// Template for a descriptor whose content does not depend on the <see cref="DescriptorContext"/>.
    /// Composers may cache descriptors built from this template at construction.
    /// </summary>
    /// <remarks>
    /// Use only when your <see cref="PgConverter.GetDescriptor"/> implementation does not read any field
    /// from the <see cref="DescriptorContext"/> (or its <see cref="PgConversionContext"/>) and returns the
    /// same descriptor on every call. If any branch of your implementation would return a context-dependent
    /// descriptor, return a plain <c>new ConverterDescriptor { BufferRequirements = ... }</c> instead. The
    /// invariant template must apply to all returns from the override, not just some of them.
    /// </remarks>
    public static ConverterDescriptor Invariant { get; } = new() { IsInvariant = true };

    public BufferRequirements BufferRequirements { get; init; }

    /// <summary>
    /// True when this descriptor was constructed from <see cref="Invariant"/>. Composers may cache such
    /// descriptors at construction, otherwise composers must re-resolve per call.
    /// </summary>
    public bool IsInvariant { get; private init; }
}
