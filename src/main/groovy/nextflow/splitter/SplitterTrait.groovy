package nextflow.splitter

import groovy.transform.CompileStatic
import org.codehaus.groovy.runtime.InvokerHelper

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class SplitterTrait {

    static final Map registry = [

            CSV: CsvSplitter.instance,
            TEXT: TextSplitter.instance,
            STRING: StringSplitter.instance,
            FASTA: FastaSplitter.instance,
            FASTQ: FastqSplitter.instance

    ]



    static getSplitter( String methodName, Object[] args ) {
        if( methodName && InvokerHelper.invokeMethod(methodName,'startWith','split') && (InvokerHelper.invokeMethod(methodName,'size',null) as int) >5 ) {
            def qualifier = InvokerHelper.invokeMethod(methodName,'substring',5)
            qualifier = InvokerHelper.invokeMethod(qualifier,'toUpperCase',null)
            return registry.get(qualifier)
        }
    }


    def splitInvoke( Closure closure, Object obj, int index ) {
        if( !closure ) return obj
        def types = closure.getParameterTypes()
        if( types.size()>1 ) {
            return closure.call(obj, index)
        }
        else {
            return closure.call(obj)
        }
    }

}
