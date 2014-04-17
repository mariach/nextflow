package nextflow.splitter

import groovy.transform.PackageScope
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowWriteChannel
import org.codehaus.groovy.runtime.InvokerHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
abstract class AbstractSplitter<T> implements SplitterStrategy {

    protected int count

    protected def into

    protected Closure closure

    protected boolean recordMode

    protected Map recordCols

    AbstractSplitter( Map opt = [:] ) {
        options(opt)
    }

    int getCount() { count }

    def getInto() { into }

    Map getRecordCols() { recordCols }

    boolean getRecordMode() { recordMode }

    abstract protected splitImpl()

    abstract protected void setTargetObject( object )

    AbstractSplitter options( Map options ) {
        closure = (Closure)options.each
        count = options.count as Integer ?: 1
        into = options.into
        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        recordMode = isTrueOrMap(options.record)

        if( options.record instanceof Map )
            recordCols = (Map)options.record

        if( recordMode && count>1 )
            throw new IllegalArgumentException("When using 'record' option 'count' cannot be greater than 1")

        return this
    }


    /**
     * Abstract splitter method
     *
     * @param object The object to be splitted
     * @param params The map holding the splitting named parameters
     * @param closure An option closure applied to each split entry
     * @return
     */
    AbstractSplitter split( obj ) {
        setTargetObject(obj)
        return this
    }

    void each( Closure closure ) {
        this.closure = closure
        splitImpl()
    }

    long count() {
        long result = 0
        closure = { result++ }
        splitImpl()
        return result
    }

    List list() {
        into = []
        (List) splitImpl()
    }

    DataflowQueue channel() {
        into = new DataflowQueue()
        (DataflowQueue) splitImpl()
    }

//
//    final xApply( Object obj, Object[] args ) {
//
//            Closure closure = null
//            Map options = null
//
//            if( args.size() == 1 ) {
//                if( args[0] instanceof Closure )
//                    closure = args[0] as Closure
//                else if( args[0] instanceof Map )
//                    options = args[0] as Map
//                else
//                    throw new IllegalArgumentException()
//            }
//            else if( args.size() == 2 ) {
//                options = args[0] as Map
//                closure = args[1] as Closure
//            }
//            else if( args.size()>2 )
//                throw new IllegalArgumentException()
//
//            if( options == null )
//                options = [:]
//
//        // finally invoke the splitter by invoking the apply method
//        apply(obj, options, closure)
//
//    }


    static getSplitter( String methodName, Object[] args ) {
        if( methodName && InvokerHelper.invokeMethod(methodName,'startWith','split') && (InvokerHelper.invokeMethod(methodName,'size',null) as int) >5 ) {
            def qualifier = InvokerHelper.invokeMethod(methodName,'substring',5)
            qualifier = InvokerHelper.invokeMethod(qualifier,'toUpperCase',null)
            //return registry.get(qualifier)
        }
    }


    @PackageScope
    static splitCall( Closure closure, Object obj, int index ) {
        if( !closure ) return obj
        def types = closure.getParameterTypes()
        if( types.size()>1 ) {
            return closure.call(obj, index)
        }
        else {
            return closure.call(obj)
        }
    }

    /**
     * Add a generic value to a target container, that can be either a {@code Collection}
     * or a {@code DataflowWriteChannel} instance
     *
     * @param into The target container, either a {@code Collection} or a {@code DataflowWriteChannel} instance
     * @param value Any value
     * @throws {@code IllegalArgumentException} whenever parameter {@code into} is not a valid object
     */
    protected void append( into, value ) {
        if( into instanceof Collection )
            into.add(value)

        else if( into instanceof DataflowWriteChannel )
            into.bind(value)

        else
            throw new IllegalArgumentException("Not a valid 'into' target object: ${into?.class?.name}")
    }

    static protected boolean isTrueOrMap( value ) {
        if( value instanceof Map )
            return true

        return value instanceof Boolean && (value as Boolean)
    }

}
