package nextflow.splitter

import groovy.util.logging.Slf4j

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@Singleton(strict = false)
class CsvSplitter extends AbstractTextSplitter {

    @Override
    def apply(Reader reader, Map options, Closure<String> closure) {
        return null
    }

}
