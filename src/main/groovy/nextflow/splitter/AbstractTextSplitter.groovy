package nextflow.splitter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.InheritConstructors
import nextflow.util.CharsetHelper
/**
 * Implements an abstract text splitting for the main types
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@InheritConstructors
abstract class AbstractTextSplitter extends AbstractSplitter<Reader> {

    protected Reader targetObject

    protected Charset charset

    Charset getCharset() { charset }

    AbstractTextSplitter options(Map options) {
        super.options(options)
        charset = CharsetHelper.getCharset(options.charset)
        return this
    }

    protected void setTargetObject( obj ) {

        if( obj instanceof Reader )
            targetObject = (Reader) obj

        else if( obj instanceof CharSequence )
            targetObject = new StringReader(obj.toString())

        else if( obj instanceof Path )
            targetObject = Files.newBufferedReader(obj, charset)

        else if( obj instanceof InputStream )
            targetObject =new InputStreamReader(obj,charset)

        else if( obj instanceof File )
            targetObject = new FileReader(obj)

        else if( obj instanceof char[] )
            targetObject = new StringReader(new String(obj))

        else
            throw new IllegalArgumentException("Object of class '${obj.class.name}' does not support 'chopString' method")

    }


}
