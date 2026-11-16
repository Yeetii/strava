using System.Reflection;

namespace Shared.Hosting;

/// <summary>
/// Functions trigger attributes live on the method parameter, not the method.
/// All trigger discovery goes through here.
/// </summary>
public static class FunctionAttributeLookup
{
    public static T? Get<T>(MethodInfo method) where T : Attribute
    {
        var onMethod = method.GetCustomAttribute<T>();
        if (onMethod is not null)
            return onMethod;

        foreach (var parameter in method.GetParameters())
        {
            var onParameter = parameter.GetCustomAttribute<T>();
            if (onParameter is not null)
                return onParameter;
        }
        return null;
    }
}
