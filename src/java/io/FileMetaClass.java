package groovy.runtime.metaclass.java.io;

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FileMetaClass { /* extends groovy.lang.DelegatingMetaClass {

    public FileMetaClass(MetaClass delegate) {
        super(delegate);
    }


    public Object invokeMethod(Object obj, String methodName, Object[] args)
    {

        if( methodName.startsWith("split") && methodName.length()> 5 ) {
            return methodName;
        }
        else {
            return  delegate.invokeMethod(obj, methodName, args);
        }

    }

 */
}
