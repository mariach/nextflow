package nextflow.splitter
import java.util.regex.Pattern

import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill
import nextflow.extension.FilesExtensions
import nextflow.util.CacheHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@InheritConstructors
class FastaSplitter extends AbstractTextSplitter {

    static private Pattern PATTERN_FASTA_DESC = ~/^>\S+\s+(.*)/

    /**
     * Parse a {@code CharSequence} as a FASTA formatted text, retuning a {@code Map} object
     * containing the fields as specified by the @{code record} parameter.
     * <p>
     *  For example:
     *  <pre>
     *  def fasta = '''
     *      >5524211 cytochrome b [Elephas maximus maximus]
     *      LCLYTHIGRNIYYGSYLYSETWNTGIMLLLITMATAFMGYVLPWGQMSFWGATVITNLFSAIPYIGTNLV
     *      IENY/
     *      '''.stripIndent()
     *
     *   def record = fasta.parseFastaRecord( [ id: true, seq: true ]
     *   assert record.id == '5524211'
     *   assert record.seq = 'LCLYTHIGRNIYYGSYLYSETWNTGIMLLLITMATAFMGYVLPWGQMSFWGATVITNLFSAIPYIGTNLVIENY'
     *  </pre>
     *
     *
     * @param fasta
     *      The fasta formatted text to be parsed
     * @param record
     *      The map object that is used to specify which fields are required to be returned in the result map.
     *      The following field can be used:
     *      <li>{@code id} The fasta ID
     *      <li>{@code seq} The sequence string
     *      <li>{@code desc} The description in the fasta header
     *      <li>{@code head} The fasta header (first line including the '>' character)
     *      <li>{@code text} The complete fasta text block
     *      <li>{@code width} The width of the fasta formatted block. If 0 is specified the sequence is not broken into multiple lines
     *      <li>{@code hash} The hashCode of the entered FASTA sequence
     *      <li>{@code uuid} A random {@code UUID} id for this sequence
     *
     *
     * @return
     */
    final static parseFastaRecord( CharSequence fasta, Map record ) {
        assert fasta != null
        if( !record ) return

        String head = null

        def body = new StringBuilder()
        fasta.eachLine { String line ->
            line = line.trim()
            if( !line ) return
            if( line.startsWith(';')) return
            if( !head ) {
                if( line.startsWith('>') )
                    head = line
                return
            }

            if( body.size() )
                body.append('\n')

            body.append(line)
        }

        def result = [:]
        if( record.id && head ) {
            def clean = head.substring(1) // 'remove the '>' char'
            int p = clean.indexOf(' ')
            result.id = p != -1 ? clean.substring(0,p) : clean
        }
        if( record.desc && head ) {
            def m = PATTERN_FASTA_DESC.matcher(head)
            result.desc = m.matches() ? m.group(1) : null
        }
        if( record.head ) {
            result.head = head
        }
        if( record.text ) {
            result.text = fasta.toString()
        }
        if( record.seq ) {
            if( record.width == null ) {
                result.seq = body.toString()
            }
            else if( record.width.toString().isInteger()) {
                def buff = new StringBuilder()
                int len = record.width as int

                if( len > 0 ) {
                    new StringSplitter()
                        .options(count:len, ignoreNewLine: true)
                        .split(body)
                        .each { str ->
                                if( buff.size() ) buff.append('\n')
                                buff.append(str)
                        }
                }
                else {
                    body.eachLine { buff.append(it) }
                }

                result.seq = buff.toString()
            }
            else {
                throw new IllegalArgumentException("Invalid 'width' argument value: ${record.width}")
            }
        }
        if( record.uuid ) {
            result.uuid = UUID.randomUUID()
        }
        if( record.hash ) {
            result.hash = CacheHelper.hasher(fasta).hash()
        }

        return result
    }


    @Override
    def splitImpl( ) {
        assert targetObject != null

        BufferedReader reader0 = (BufferedReader)(targetObject instanceof BufferedReader ? targetObject : new BufferedReader(targetObject))

        def result = null
        String line
        StringBuilder buffer = new StringBuilder()
        int index = 0
        int blockCount=0
        boolean openBlock = false

        try {

            while( (line = reader0.readLine()) != null ) {

                if( line.startsWith(';')) continue

                if ( line == '' ) {
                    buffer << '\n'
                }
                else if ( !openBlock && line.charAt(0)=='>' ) {
                    openBlock = true
                    buffer << line << '\n'
                }
                else if ( openBlock && line.charAt(0)=='>') {
                    // another block is started

                    if ( ++blockCount == count ) {
                        // invoke the closure, passing the read block as parameter
                        def record = recordMode ? parseFastaRecord(buffer.toString(), recordCols) : buffer.toString()
                        result = splitCall( closure, record, index++ )
                        if( into != null ) {
                            append(into, result)
                        }

                        buffer.setLength(0)
                        blockCount=0
                    }

                    buffer << line << '\n'

                }
                else {
                    buffer << line << '\n'
                }
            }

        }
        finally {
            FilesExtensions.closeQuietly(reader0)
        }


        /*
         * if there's something remaining in the buffer it's supposed
         * to be the last entry
         */
        if ( buffer.size() ) {
            def record = recordMode ? parseFastaRecord(buffer.toString(), recordCols) : buffer.toString()
            result = splitCall(closure, record, index )
            if( into != null )
                append(into, result)
        }

        /*
         * now close and return the result
         * - when the target it's a channel, send stop message
         * - when it's a list return it
         * - otherwise return the last value
         */
        if( into instanceof DataflowWriteChannel ) {
            append(into, PoisonPill.instance)
            return into
        }
        if( into != null )
            return into

        return result
    }

}
