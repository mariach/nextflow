package groovy.runtime.metaclass.java.lang;

import groovy.lang.MetaClass;
import nextflow.splitter.AbstractBinarySplitter;
import nextflow.splitter.AbstractTextSplitter;
import nextflow.splitter.SplitterTrait;

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
public class StringMetaClass extends groovy.lang.DelegatingMetaClass {


    public StringMetaClass(MetaClass delegate) {
        super(delegate);
    }


    @Override
    public Object invokeMethod(Object obj, String methodName, Object[] args)
    {

        Object splitter = SplitterTrait.getSplitter(methodName, args);
        if( splitter != null ) {
            if( splitter instanceof AbstractTextSplitter ) {
                return ((AbstractTextSplitter) splitter).apply(delegate, args);
            }

            if( splitter instanceof AbstractBinarySplitter ) {
                return ((AbstractBinarySplitter) splitter).apply(delegate, args);
            }

            throw new IllegalArgumentException();
        }

        // fallback on the default
        return delegate.invokeMethod(obj, methodName, args);

    }

}
