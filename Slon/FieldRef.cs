using System.Runtime.CompilerServices;

namespace Slon;

// This is safe against unloading as long as instance is the same type as the getter function is defined on.
// There is unfortunately no static type safety to guarantee a user creates one correctly.
readonly struct FieldRef<T>
{
    readonly unsafe delegate*<object, ref T> _getter;
    readonly object _instance;

    unsafe FieldRef(delegate*<object, ref T> getter, object instance)
    {
        _getter = getter;
        _instance = instance;
    }

    public object Instance => _instance;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ref T Invoke()
    {
        unsafe
        {
            return ref _getter(_instance);
        }
    }

    public static unsafe FieldRef<T> Create<TInstance>(delegate*<TInstance, ref T> getter, TInstance instance) where TInstance : class
        => new((delegate*<object, ref T>)getter, instance);
}
