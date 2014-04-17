package nextflow.splitter
import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.InheritConstructors
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


@InheritConstructors
abstract class AbstractBinarySplitter extends AbstractSplitter<InputStream> {

    protected InputStream targetObject

    protected void setTargetObject( obj ) {

        if( obj instanceof InputStream )
            targetObject = (InputStream) obj

        else if( obj instanceof byte[] )
            targetObject = new ByteArrayInputStream(obj)

        else if( obj instanceof CharSequence )
            targetObject = new ByteArrayInputStream(obj.toString().bytes)

        else if( obj instanceof Path )
            targetObject = Files.newInputStream(obj)

        else if( obj instanceof File )
            targetObject = new FileInputStream(obj)

        else
            throw new IllegalAccessException("Object of class '${obj.class.name}' does not support 'chopBytes' method")

    }


}
