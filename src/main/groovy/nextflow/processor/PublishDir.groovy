/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.processor
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.PackageScope
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
/**
 * Implements the {@code publishDir} directory. It create links or copies the output
 * files of a given task to a user specified directory.
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@ToString
@EqualsAndHashCode
class PublishDir {

    enum Mode { SYMLINK, LINK, COPY, MOVE }

    private Map<Path,Boolean> makeCache = new HashMap<>()

    /**
     * The target path where create the links or copy the output files
     */
    Path path

    /**
     * Whenever overwrite existing files
     */
    Boolean overwrite

    /**
     * The publish {@link Mode}
     */
    Mode mode

    /**
     * A glob file pattern to filter the files to be published
     */
    String pattern

    /**
     * SaveAs closure. Allows the dynamically definition of published file names
     */
    Closure saveAs

    private PathMatcher matcher

    private FileSystem sourceFileSystem

    private TaskProcessor processor

    private Path sourceDir

    private static ExecutorService executor

    void setPath( Closure obj ) {
        setPath( obj.call() as Path )
    }

    void setPath( String str ) {
        setPath(str as Path)
    }

    void setPath( Path obj ) {
        this.path = obj.complete()
    }

    void setMode( String str ) {
        this.mode = str.toUpperCase() as Mode
    }

    void setMode( Mode mode )  {
        this.mode = mode
    }

    /**
     * Object factory method
     *
     * @param obj When the {@code obj} is a {@link Path} or a {@link String} object it is
     * interpreted as the target path. Otherwise a {@link Map} object matching the class properties
     * can be specified.
     *
     * @return An instance of {@link PublishDir} class
     */
    static PublishDir create( obj ) {
        def result
        if( obj instanceof Path ) {
            result = new PublishDir(path: obj)
        }
        else if( obj instanceof List ) {
            result = createWithList(obj)
        }
        else if( obj instanceof Map ) {
            result = createWithMap(obj)
        }
        else if( obj instanceof CharSequence ) {
            result = new PublishDir(path: obj)
        }
        else {
            throw new IllegalArgumentException("Not a valid `publishDir` directive: ${obj}" )
        }

        if( !result.path ) {
            throw new IllegalArgumentException("Missing path in `publishDir` directive")
        }

        return result
    }

    static private PublishDir createWithList(List entry) {
        assert entry.size()==2
        assert entry[0] instanceof Map

        def map = new HashMap((Map)entry[0])
        map.path = entry[1]

        createWithMap(map)
    }

    static private PublishDir createWithMap(Map map) {
        assert map

        def result = new PublishDir()
        if( map.path )
            result.path = map.path

        if( map.mode )
            result.mode = map.mode

        if( map.pattern )
            result.pattern = map.pattern

        if( map.overwrite != null )
            result.overwrite = map.overwrite

        if( map.saveAs )
            result.saveAs = map.saveAs

        return result
    }

    /**
     * Apply the publishing process to the specified {@link TaskRun} instance
     *
     * @param task The task whose output need to be published
     */
    @CompileStatic
    void apply( List<Path> files, TaskRun task ) {

        if( !files ) {
            return
        }

        this.processor = task.processor
        this.sourceDir = task.workDir
        this.sourceFileSystem = sourceDir.fileSystem

        createPublishDir()

        validatePublishMode()

        /*
         * when the publishing is using links, create them in process
         * otherwise copy and moving file can take a lot of time, thus
         * apply the operation using an external thread
         */
        final inProcess = mode == Mode.LINK || mode == Mode.SYMLINK

        if( pattern ) {
            this.matcher = FileHelper.getPathMatcherFor("glob:${pattern}", sourceFileSystem)
        }

        if( !inProcess ) {
            createExecutor()
        }

        /*
         * iterate over the file parameter and publish each single file
         */
        files.each { value ->
            apply(value, inProcess)
        }
    }


    @CompileStatic
    protected void apply( Path source, boolean inProcess ) {

        def target = sourceDir.relativize(source)
        if( matcher && !matcher.matches(target) ) {
            // skip not matching file
            return
        }

        if( saveAs && !(target=saveAs.call(target.toString()))) {
            // skip this file
            return
        }

        final destination = resolveDestination(target)
        if( inProcess ) {
            processFile(source, destination)
        }
        else {
            executor.submit({ safeProcessFile(source, destination) } as Runnable)
        }

    }

    @CompileStatic
    protected Path resolveDestination(target) {

        if( target instanceof Path ) {
            if( target.isAbsolute() ) {
                return (Path)target
            }
            // note: convert to a string to avoid `ProviderMismatchException` when the
            // destination `path` is not a unix file system
            return path.resolve(target.toString())
        }

        if( target instanceof CharSequence ) {
            return path.resolve(target.toString())
        }

        throw new IllegalArgumentException("Not a valid publish target path: `$target` [${target?.class?.name}]")
    }

    @CompileStatic
    protected void safeProcessFile(Path source, Path destination) {
        try {
            processFile(source, destination)
        }
        catch( Throwable e ) {
            log.error """
                Unxpected condition while publishing file:
                  mode  : ${mode.toString().toLowerCase()}
                  source: $source
                  target: ${destination.toUri()}
                  cause : ${e.toString()}""".stripIndent().trim(), e
            (Global.session as Session).abort(e)
        }
    }

    @CompileStatic
    protected void processFile( Path source, Path destination ) {

        // create target dirs if required
        makeDirs(destination.parent)

        try {
            processFileImpl(source, destination)
        }
        catch( FileAlreadyExistsException e ) {
            if( !overwrite )
                return

            FileHelper.deletePath(destination)
            processFileImpl(source, destination)
        }
    }

    @CompileStatic
    protected void processFileImpl( Path source, Path destination ) {
        if( log.isTraceEnabled())
            log.trace "publishing file: $source -[$mode]-> $destination"

        if( !mode || mode == Mode.SYMLINK ) {
            Files.createSymbolicLink(destination, source)
        }
        else if( mode == Mode.LINK ) {
            FilesEx.mklink(source, [hard:true], destination)
        }
        else if( mode == Mode.MOVE ) {
            FilesEx.moveTo(source, destination)
        }
        else if( mode == Mode.COPY ) {
            FilesEx.copyTo(source, destination)
        }
        else {
            throw new IllegalArgumentException("Unknown file publish mode: ${mode}")
        }
    }

    @CompileStatic
    private void createPublishDir() {
        makeDirs(this.path)
    }

    @CompileStatic
    private void makeDirs(Path dir) {
        if( !dir || makeCache.containsKey(dir) )
            return

        try {
            Files.createDirectories(dir)
        }
        catch( FileAlreadyExistsException e ) {
            // ignore
        }
        finally {
            makeCache.put(dir,true)
        }
    }

    /*
     * That valid publish mode has been selected
     * Note: link and symlinks are not allowed across different file system
     */
    @CompileStatic
    @PackageScope
    void validatePublishMode() {

        if( sourceFileSystem != path.fileSystem ) {
            if( !mode ) {
                mode = Mode.COPY
            }
            else if( mode == Mode.SYMLINK || mode == Mode.LINK ) {
                mode = Mode.COPY
                if( processor )
                    processor.warn("Cannot use mode `${mode.toString().toLowerCase()}` to publish files to path: $path -- Using mode `copy` instead")
            }
        }

        if( !mode ) {
            mode = Mode.SYMLINK
        }
    }

    @CompileStatic
    static synchronized private void createExecutor() {
        if( !executor ) {
            executor = new ThreadPoolExecutor(0, Runtime.runtime.availableProcessors(),
                    60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());

            // register the shutdown on termination
            def session = Global.session as Session
            if( session ) {
                session.onShutdown {
                    executor.shutdown()
                    executor.awaitTermination(36,TimeUnit.HOURS)
                }
            }
        }
    }

    @PackageScope
    static ExecutorService getExecutor() { executor }
}