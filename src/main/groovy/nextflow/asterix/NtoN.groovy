package nextflow.asterix

/**
 * Created by maria chatzou (mariach) on 02/07/16.
 */
class NtoN {

    //Get files in folder ....
    //run command ...
    //output to folder ...

    def nat_lan = [
            "input"         : "input",
            "in_dir"        : "input",
            "in_channel"    : "input",
            "in_file"       : "input",
            "cmd"           : "cmd",
            "output"        : "output",
            "out_dir"       : "output",
            "out_file"      : "output",
            "out_channel"   : "output"
    ];

    def nat_lan_input = [
            "get"       : "in",
            "take"      : "in",
            "use"       : "in",
            "files"     : "in_dir",
            "file"      : "in_file",
            "folder"    : "in_dir",
            "memory"    : "in_channel",
            "channel"   : "in_channel",
    ];

    def nat_lan_input_fingerprint = [
            "git "       : "get",
            "got "       : "get",
            "tak "       : "take",
            "toke "      : "take",
            "takee "     : "take",
            "usse "      : "use",
            "us "        : "use",
            "se "        : "use",
            "filles"     : "files",
            "fils "      : "files",
            "foulder"    : "folder",
            "foldr"      : "folder",
            "fulder"     : "folder",
            "fil "       : "file",
            "fille "     : "file",
            "chanel "    : "channel",
            "chanell"    : "channel",
            "shannel"    : "channel",
            "mmory"      : "memory",
            "memmory"    : "memory",
            "memori"     : "memory"
    ];

    def nat_lan_cmd = [
            "run"       : "cmd",
            "execute"   : "cmd",
            "command"   : "cmd",
            "program"   : "cmd",
            "script"    : "cmd"
    ];

    def nat_lan_output = [
            "output"    : "out",
            "write"     : "out",
            "files"     : "out_dir",
            "folder"    : "out_dir",
            "file"      : "out_dir",
            "memory"    : "out_channel",
            "channel"   : "out_channel"
    ];

    def nat_lan_output_fingerprint = [
            "outpt"     : "output",
            "otput"     : "output",
            "ouput"     : "output",
            "writ"      : "write",
            "rite"      : "write",
            "filles"    : "files",
            "fils "     : "files",
            "foulder"   : "folder",
            "foldr"     : "folder",
            "fulder"    : "folder",
            "fil "      : "file",
            "fille "    : "file",
            "chanel "   : "channel",
            "chanell"   : "channel",
            "shannel"   : "channel",
            "mmory"     : "memory",
            "memmory"   : "memory",
            "memori"    : "memory"
    ];

    def nat_lan_config =[
            "executor"      : "executor",
            "cpus"          : "cpus",
            "threads"       : "cpus",
            "thread"        : "cpus",
            "cores"         : "cpus",
            "core"          : "cpus",
            "memory"        : "memory",
            "queue"         : "queue",
            "module"        : "module" ,
            "scratch"       : "scratch",
            "docker-image"  : "docker",
            "docker"        : "docker",
            "image"         : "docker"
    ];

    def nat_lan_config_fingerprint =[
            "executo"       : "executor",
            "executr"       : "executor",
            "execuor"       : "executor",
            "exector"       : "executor",
            "exeutor"       : "executor",
            "excutor"       : "executor",
            "eecutor"       : "executor",
            "xecutor"       : "executor",
            "cpu"           : "cpus",
            "pus"           : "cpus",
            "threds"        : "threads",
            "treads"        : "threads",
            "treds"         : "threads",
            "thred"         : "thread",
            "tread"         : "thread",
            "tred"          : "thread",
            "cres"          : "cores",
            "cors"          : "cores",
            "cre"           : "core",
            "cor"           : "core",
            "mem"           : "memory",
            "memor"         : "memory",
            "mmory"         : "memory",
            "meory"         : "memory",
            "memry"         : "memory",
            "queu"          : "queue",
            "qeu"           : "queue",
            "que"           : "queue",
            "modul"         : "module",
            "mdule"         : "module",
            "modue"         : "module",
            "odule"         : "module",
            "scrath"        : "scratch",
            "scratc"        : "scratch",
            "scrutch"       : "scratch",
            "cratch"        : "scratch",
            "doker-image"   : "doker-image",
            "docer-image"   : "doker-image",
            "dcker-image"   : "doker-image",
            "docke-image"   : "doker-image",
            "docker-img"    : "doker-image",
            "docer-img"     : "doker-image",
            "dcker-img"     : "doker-image",
            "docke-img"     : "doker-image",
            "doker"         : "docker",
            "docer"         : "docker",
            "dcker"         : "docker",
            "docke"         : "docker",
            "img"           : "image",
            "imge"          : "image",
            "mage"          : "image",
            "imag"          : "image"
    ];
//params.base_dir="/users/cn/mhatzou/Datasets/HomFam/seqs/";
//params.out_dir="MEGA_NF";



    def nf_key =[
            "input"  : 'params.in',
            "output" : 'params.out',
            "processName" : "\nprocess p",
            "publish_dir" : "\tpublishDir ",
            "processIn" : "\t"+'file(input) from Channel.fromPath('
    ];

    Map <Integer, String> nf_skeleton  =[
//            1 : "input",
//            2 : "output",
            3 : "processName",
            4 : "processStart",
            5 : "publish_dir",
            6 : "processNameIn",
            7 : "processIn",
            8 : "processIn2",
            9 : "processNameOut",
            10 : "processOut",
            11 : "scriptStart",
            12 : "cmd",
            13 : "scriptEnd",
            14 : "processEnd"
    ];

    def nf_lan=[
            "processStart" : "{\n\n",
            "processEnd" : "}\n\n",
            "processNameIn" : "\n\tinput:\n",
            "processNameOut" : "\n\toutput:\n",
            "scriptStart" : "\n\t"+'"""'+"\n",
            "scriptEnd" : "\t"+'"""'+"\n",
            "processIn2" : ')'+"\n",
            //"processOut" : "\t"+'file "${input}.out"'+"\n"
    ];

    def nf_process=[];



    /*
     * Get config parameters
     */
    private getParam( def param_str ){

        param_str = param_str.replaceAll("'", '"') //Replace single quotes to double ones, so that pattern matching always works
        param_str = param_str.replaceAll(";", ',') //Replace ; to , because NXF understands ,
        param_str = param_str.replaceAll("using", "")

        def param_found=0
        def param=""
        def val=""

        nat_lan_config.each(){ key, value ->

            if( param_str.toLowerCase().contains(key) )
            {
                def m = param_str =~ /.*?\"(.*)\".*/
                if (m.matches())
                      val = '"' + m.group(1) + '"'
                else
                {
                    param_str = param_str.replaceAll(key, "")
                    param_str = param_str.replaceAll("of", "")
                    param_str = param_str.replaceAll("off", "")
                    param_str = param_str.replaceAll("the", "")
                    param_str = param_str.replaceAll("available", "")
                    param_str = param_str.replaceAll("possible", "")
                    param_str = param_str.replaceAll("existing", "")
                    param_str = param_str.replaceAll(" ", "")
                    param_str = param_str.replaceAll("=", "")

                    if( !param_str.matches("[A-Za-z0-9]+") )
                        println "A non-alhanumeric character was found in '" + param_str + "'. Please correct!"
                    param_str = param_str.replaceAll("[^A-Za-z0-9]", "")

                    val=param_str
                }

                param = value
                param_found = 1
            }

            if( param_found == 1  ){
                return true
            }
        }

        if ( param_found == 0 ){
            nat_lan_config_fingerprint.each() { key, value ->

                if( param_str.toLowerCase().contains(key) )
                {
                    println "\nDid you mean '" + value + "' ?"
                    println "If YES, please correct '" + key + "' to '" + value + "' ."
                    println "If NO, please change '" + key + "' to sth different, since it is currently being recognized and used as '" + value + "' .\n"

                    def m = param_str =~ /.*?\"(.*)\".*/
                    if (m.matches())
                        val = '"' + m.group(1) + '"'
                    else
                    {
                        if( !param_str.matches("[A-Za-z0-9]+") )
                            println "A non-alhanumeric character was found in '" + param_str + "'. Please correct!"
                        param_str = param_str.replaceAll("[^A-Za-z0-9]", "")

                        param_str = param_str.replaceAll(key, "")
                        param_str = param_str.replaceAll("of", "")
                        param_str = param_str.replaceAll("off", "")
                        param_str = param_str.replaceAll("the", "")
                        param_str = param_str.replaceAll("available", "")
                        param_str = param_str.replaceAll("possible", "")
                        param_str = param_str.replaceAll("existing", "")
                        param_str = param_str.replaceAll(" ", "")
                        param_str = param_str.replaceAll("=", "")

                        val=param_str
                    }

                    param = nat_lan_config[value]
                    param_found = 1
                }

                if( param_found == 1  ){
                    return true
                }
            }
        }

        return [val, param]
    }



    /*
     * Get config
     */
    private getConfig( def using_str )
    {
        def using_ar=using_str.split("\n")

        def config=[:]

        //for each using line
        for( int i=0; i<using_ar.size(); i++)
        {
            def using_tmp=using_ar[i].tokenize(',')

            //for each parameter in the line seperated by comma
            for( int j=0; j<using_tmp.size(); j++)
            {
                def value = ""
                def param = ""
                (value, param) = getParam(using_tmp[j])

                if (param == 'executor' && !value.contains('"'))
                    config[param] = '"'+ value + '"'
                else if (param == 'scratch')
                    config[param] = 'true'
                else if (param == 'docker')
                {
                    if (value == null || value.isEmpty()) {
                        println "ERROR! The docker image has not been defined!\nPlease specify a docker image and re-run.\n"; return;
                    }
                    config['container'] = value
                    config['docker.enabled'] = 'true'
                }
                else if (param == 'trace')
                    config['trace.enabled'] = 'true'
                else
                    config[param] = value
            }
        }

        return config
    }



    /*
     * Print config
     */
    private printConfig( def config_all )
    {
        def config_string=""

        config_string = "\nprocess {\n"

        config_all.sort().each{ key, value ->
            if( ! key.contains('.') )
                config_string += "\t\t" + key + " = " + value + "\n"
        }

        config_string += "\n"

        config_all.sort().each{ key, value ->
            if( key.contains('.') && key != 'docker.enabled' && key != 'trace.enabled' )
                config_string += "\t\t" + key + " = " + value + "\n"
        }
        config_string += "}\n"

        if (config_all.containsKey('docker.enabled'))
            config_string += "\n" + 'docker.enabled = true'
        if (config_all.containsKey('trace.enabled'))
            config_string += "\n" + 'trace.enabled = true'

        return config_string
    }



    /*
     * Get input type and in-channel names
     */
    private getInput( def param_str ) {

        param_str = param_str.toLowerCase()

        def input_found=0
        def in_type=""
        def name=""

        nat_lan_input_fingerprint.each{ key, value ->

            if( param_str.contains(key) )
            {
                println "\nIn : " + param_str
                println "\ndid you mean '" + value + "' ?"
                println "If YES, please correct '" + key + "' to '" + value + "' ."
                println "If NO, please change '" + key + "' to sth different, since it is currently being recognized and used as '" + value + "' .\n"

                param_str = param_str.replaceAll(key, value)
            }
        }

        nat_lan_input.each(){ key, value ->

            if( param_str.contains(key) )
                input_found ++
        }

        if ( input_found < 1 ) {
            println "ERROR! Wrong input task definition!\n"; return;
        }


        if(param_str.contains("files") || param_str.contains("folder"))
            in_type="in_dir"

        else if(param_str.contains("channel") || param_str.contains("memory") )
            in_type="in_channel"

        else if(param_str.contains("file") )
            in_type="in_file"

        else{
            println "ERROR! Unable to understand input type!\n"; return;
        }

        param_str = param_str.replaceAll("get", "")
        param_str = param_str.replaceAll("take", "")
        param_str = param_str.replaceAll("files", "@")
        param_str = param_str.replaceAll("file", "@")
        param_str = param_str.replaceAll("folder", "@")
        param_str = param_str.replaceAll("in", "@")
        param_str = param_str.replaceAll("from", "@")

        def m = param_str=~ /(.*?)@.*/
        if ( m.matches() ) {
            name = m.group(1)
            name.replaceAll(" ","")

            if( name.trim().isEmpty() )
                name="null"
        }

        return [name, in_type]
    }



    /*
     * Get output type and out-channel names
     */
    def getOutput( def param_str ){

        def m=param_str.tokenize(' ')
        def name = m[m.size()-1]

        param_str = param_str.toLowerCase()

        def output_found=0
        def out_type=""


        nat_lan_output_fingerprint.each{ key, value ->

            if( param_str.contains(key) )
            {
                println "\nIn : " + param_str
                println "\ndid you mean '" + value + "' ?"
                println "If YES, please correct '" + key + "' to '" + value + "' ."
                println "If NO, please change '" + key + "' to sth different, since it is currently being recognized and used as '" + value + "' .\n"

                param_str = param_str.replaceAll(key, value)
            }
        }

        nat_lan_output.each(){ key, value ->

            if( param_str.contains(key) )
                output_found ++
        }

        if ( output_found < 1 ) {
            println "ERROR! Wrong input task definition!\n"; return;
        }


        if(param_str.contains("files") || param_str.contains("folder")) {
            out_type = "out_dir"
            name="null"
        }

        else if(param_str.contains("channel") || param_str.contains("memory") )
            out_type = "out_channel"

        else{
            println "ERROR! Unable to understand output type!\n"; return;
        }

        param_str = param_str.replaceAll("write", "")
        param_str = param_str.replaceAll("output", "")
        param_str = param_str.replaceAll("files", "")
        param_str = param_str.replaceAll("to", "")
        param_str = param_str.replaceAll("the", "")
        param_str = param_str.replaceAll("folder", "")
        param_str = param_str.replaceAll("memory", "")
        param_str = param_str.replaceAll("in", "")
        param_str = param_str.replaceAll("channel", "")
        param_str = param_str.replaceAll("named", "")
        param_str = param_str.replaceAll("called", "")
        param_str = param_str.replaceAll("\n", "")

        println param_str

        if (param_str.trim().isEmpty()){
            println "ERROR! Most probably missing output name!\n"; return;
        }

        return [name, out_type]
    }



    /*
     * Get process.
     * Returns process elements and tags each line either as "input", "output" or "cmd" based on sentence meaning.
     */
    private getProcess( def process_str, def proc )
    {
        def process          = process_str.tokenize("\n")
        def process_status   = [:]
        def process_body     = [:]
        def process_output   = [:]
        def process_inname   = [:]
        def process_outname  = [:]
        def process_config   = [:]

        def out_ar=[:]
        def inname_ar=[:]
        def in_counter=0;
        def outname_ar=[:]
        def out_counter=0;
        String cmd =""
        def status=""

        for( int i=0; i<process.size(); i++)
        {
            def entry=process[i];
            if(entry)
            {

                def m3=entry=~/^using(.*)/
                if(m3.matches())
                {
                    status="using"

                    def config_proc_tmp = [:]
                    config_proc_tmp = getConfig(entry)

                    config_proc_tmp.each { key, value ->
                        if(!key.contains("."))
                            process_config[ '$p' + proc + "." + key ] = value
                        else
                            process_config[ key ] = value
                    }
                }
                else {

                    def m = entry =~ /^(.*?)\"(.*)\"/
                    if (m.matches()) {
                        // m.group(1) contains all the nat lan proceeding the command
                        cmd = m.group(2);
//                        cmd = cmd.replace('$Out.', '${input}.');
//                        cmd = cmd.replace('$Out', '${input}.out');
//                        cmd = cmd.replace('$In', '$input');

                        cmd = cmd.replaceAll("Out\\.([A-Za-z0-9]+)", "{input_\$1}.out");
                        println cmd
                        status = "cmd";
                        cmd = cmd + " "
                        process_body[i + 1] = cmd;

                        def ins = cmd.split('input}.')     // /(\$\{input\}\..*?)['|\n|"|;|>|\s+]/
                        for (int t = 1; t < ins.size(); t++) {

                            ins[t] = ins[t].replace('${', " ")
                            ins[t] = ins[t].replaceAll("'", " ")
                            ins[t] = ins[t].replaceAll('"', " ")
                            ins[t] = ins[t].replaceAll(';', " ")
                            ins[t] = ins[t].replaceAll('>', " ")

                            def outs = '${input}.' + ins[t]

                            def m2 = outs =~ /(\$\{input\}\..*?)['|\n|"|;|>|\s+]/
                            if (m2.matches()) {
                                def matched = m2.group(1).replaceAll(' ', ""); //println "M: " + matched;
                                if (out_ar.containsKey(matched)) {
                                    out_ar[matched]++
                                } else {
                                    out_ar[matched] = 1
                                }
                            }
                        }
                    } else {

                        if (cmd) {
                            out_counter++;
                            status = "output";
                            def output_name="";

                            (output_name, status)= getOutput(entry);      //println "O: " + output_name + " " +status;

                            if( !outname_ar[output_name] )
                                outname_ar[output_name]=proc + "_out" + out_counter
                            else{
                                println "ERROR! Multiple outputs have the same name!\n"; return;
                            }
                        }
                        else {
                            in_counter++;
                            status = "input";
                            def input_name="";

                            (input_name, status)= getInput(entry);      //println "I: " + input_name + " " +status;

                            if( !inname_ar[input_name] )
                                inname_ar[input_name]=proc + "_in" + in_counter
                            else{
                                println "ERROR! Multiple inputs have the same name!\n"; return;
                            }
                        }

                        def words = entry.tokenize(' ');
                        process_body[i + 1] = words;
                    }
                }
                process_status[i+1]=status;
            }
        }
        process_output  = out_ar
        process_inname  = inname_ar
        process_outname = outname_ar

        return [process_status, process_body, process_output, process_inname, process_outname, process_config];
    }


    /*
     * Get IO
     */
    private getNxfIO( def process_all_status,  def process_all_body )
    {
        def process_all_io=[:];
        def process_num_io=[:];

        process_all_status.sort().each { key, value ->

            def process_body = process_all_body[key];
            def process_status = process_all_status[key];

            def out_counter=0;
            def in_counter=0;

            //--- For all lines ---//
            process_body.sort().each { line, value2 ->

                def status = process_status[line]; print status + "\n";
                def nf_out1 = "";
                def nf_out2 = "";
                def nf_in = "";
                def nf_in1 = "";
                def nf_in2 = "";
                def words = process_body[line];

                //---- INPUT ----//
                if ( nat_lan[status] == "input")
                {
                    in_counter++;

                    nf_in = nf_key["input"];
                    if ( status == "in_dir" ) {
                        nf_in1 = nf_in + "_dir_${key}_$in_counter";
                        nf_in2 = ' = ' + '"' + words[words.size() - 1] + '/*"' + "\n";
                    }
                    if ( status == "in_file" ) {
                        nf_in1 = nf_in + "_file_${key}_$in_counter"
                        nf_in2 = ' = ' + '"' + words[words.size() - 1] + '"' + "\n";
                    }

                    if(!process_all_io[nf_in2])
                        process_all_io[nf_in2]=nf_in1

                    def key_in=key + "_in" + in_counter
                    process_num_io[key_in]=nf_in2;       //println "*** " + process_all_io[process_num_io[key_in]] +" "+ process_num_io[key_in] +" "+ key_in + "  " + nf_in2
                }

                //---- OUTPUT ----//
                if ( nat_lan[status] == "output")
                {
                    out_counter++;

                    nf_out1 = nf_key["output"] + "_dir_${key}_$out_counter";
                    nf_out2 = ' = ' + '"' + words[words.size() - 1] + '"' + "\n";

                    if(!process_all_io[nf_out2])
                        process_all_io[nf_out2]=nf_out1

                    def key_out=key+"_out"+out_counter
                    process_num_io[key_out]=nf_out2;     //println process_all_io[process_num_io[key_out]] + process_num_io[key_out] + " "+ key_out
                }
            }
        }

        return [process_all_io, process_num_io]
    }



    /*
     * Translation of Natural language to Nextflow
     */
    private translateTextToNxf( def process_all_status,  def process_all_body, def process_all_io, def process_num_io, def process_all_out  )
    {
        def file_string="";

//        process_all_out.sort().each { key, value ->
//            println "ALL OUT: " + value + " " + key
//        }

        //Printing IO
        process_all_io.sort().each { key, value ->
            file_string += value + " " + key
        }

        //--- For all processes ---//
        process_all_status.sort().each { key, value ->

            def in_counter=0
            def out_counter=0
            def tmp_cmd = ""
            def process_body = process_all_body[key]
            def process_status = process_all_status[key]

            nf_lan["processName"] = nf_key["processName"] + key;

            //--- For all lines ---//
            process_body.sort().each { line, value2 ->

                def status = process_status[line]; //print status + "\n";

                //---- INPUT ----//
                if ( nat_lan[status] == "input" )
                {
                    in_counter++;
                    def words = process_body[line];

                    def key_in=key + "_in" + in_counter
                    nf_lan["processIn"] = nf_key["processIn"] + process_all_io[process_num_io[key_in]] ;

                    println "HERE:  "+key_in + " " + process_num_io[key_in]+ " "+process_all_io[process_num_io[key_in]]
                }

                //---- OUTPUT ----//
                else if ( nat_lan[status] == "output" ) {
                    out_counter++;
                    def words = process_body[line];

                    def key_out=key + "_out" + out_counter
                    nf_lan["publish_dir"] = nf_key["publish_dir"] + process_all_io[process_num_io[key_out]] + "\n"
                }

                //---- CMD ----//
                else if ( nat_lan[status] == "cmd" ) {
                    tmp_cmd += "\t\t" + process_body[line] + "\n";
                    nf_lan["cmd"] = tmp_cmd
                }

                else {
                    println "ERROR ~ status = " + status + "! The word does not match any of the nat_lan keys!"; return
                }
            }

            //Print process
            nf_skeleton.sort().each { key2, value3 ->
                if(key2==10){
                    def out_ar=process_all_out[key];
                    out_ar.each { out_key, out_value ->
                        file_string += "\t" + 'file "'+ out_key+ '"' +"\n"
                    }
                }
                else{
                    file_string += nf_lan[nf_skeleton[key2]]
                }
            }
        }

        return file_string
    }



    /****************************************************
     *           Read file and translate to Nxf         *
     ****************************************************/

    def translate(in_file, out_file) {

        //Read text file
        def REFseq = new File("$in_file").text

        //Get processes
        def proc_conf= REFseq.split("\n\nusing")
        def lanFile = proc_conf[0].split("\n\n")

        def config_all = [:]
        if(proc_conf.size()==2){
            def useFile = proc_conf[1]

            config_all = getConfig( useFile )
        }

        def workflow            = [:]
        def process_all_status  = [:]
        def process_all_body    = [:]
        def process_all_out     = [:]
        def process_all_inname  = [:]
        def process_all_outname = [:]
        def process_all_io      = [:]
        def process_num_io      = [:]
        def proc = 0

        for (int i = 0; i < lanFile.size(); i++)
        {
            def m = lanFile[i] =~ /^\s*$/
            if (!m.matches())
            {
                proc++;
                def process_status  = [:]
                def process_body    = [:]
                def process_out     = [:]
                def process_inname  = [:]
                def process_outname = [:]
                def process_config  = [:]

                (process_status, process_body, process_out, process_inname, process_outname, process_config) = getProcess(lanFile[i], proc);

                process_all_status[ proc ]  = process_status
                process_all_body[ proc ]    = process_body
                process_all_out[ proc ]     = process_out
                process_all_inname[ proc ]  = process_inname
                process_all_outname[ proc ] = process_outname

                if( process_config )
                    process_config.each { key, value ->
                        config_all[key] = value
                    }
            }
        }

        (process_all_io, process_num_io) = getNxfIO(process_all_status, process_all_body)

        process_all_io.each() { key, value ->
            println value + " = " + key
        }

        process_num_io.each() { key, value ->
            println key + " = " + value
        }

        def file_string = "";
        file_string = translateTextToNxf( process_all_status, process_all_body, process_all_io, process_num_io, process_all_out);

        //Write files
        if( config_all )
        {
            def config_string = "";
            config_string = printConfig(config_all)
            new File("nextflow.config").text = config_string
        }

        new File("$out_file").text = file_string
    }
}
