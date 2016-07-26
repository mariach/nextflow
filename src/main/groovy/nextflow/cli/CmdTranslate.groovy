package nextflow.cli

import com.beust.jcommander.Parameter
import nextflow.asterix.NtoN
import nextflow.exception.AbortOperationException

/**
 * Created by mariach on 02/07/16.
 */
class CmdTranslate extends CmdBase {

    static final NAME = 'translate'

    @Override
    final String getName() { NAME }

    @Parameter(description = 'command name', arity = 1)
    List<String> args

    @Override
    void run() {
        def nxf_pipeline = new NtoN();

        if(args.size()==2) {
            nxf_pipeline.translate(args[0], args[1])
        }
        else{
            throw new AbortOperationException("This expects as arguments the name of a text file describing the pipeline in natural language and the name of the nextflow pipeline to write to!")
        }
    }

}
