package nextflow.splitter

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

abstract class AbstractBinarySplitter extends SplitterTrait {

    abstract apply(InputStream stream, Map options, Closure<byte[]> closure)

    def apply( object, Object[] args ) {

    }

}
