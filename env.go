package gopark

import (
    "flag"
    "fmt"
    "log"
    "os"
    "path"
    "time"
)

type _Environment struct {
    master        string
    parallel      int
    goparkWorkDir string
    jobWorkDir    string
    started       bool
    verbose       bool
}

func (e *_Environment) getLocalShufflePath(shuffleId int64, inputId, outputId int) string {
    e.start()
    pathName := path.Join(e.jobWorkDir, fmt.Sprintf("shuffle-%d", shuffleId))
    if _, err := os.Stat(pathName); os.IsNotExist(err) {
        os.Mkdir(pathName, os.ModePerm)
    }
    return path.Join(pathName, fmt.Sprintf("%05d_%05d", inputId, outputId))
}

func (e *_Environment) getLocalRDDPath(rddId int64, splitId int) string {
    e.start()
    pathName := path.Join(e.jobWorkDir, fmt.Sprintf("rdd-%d", rddId))
    if _, err := os.Stat(pathName); os.IsNotExist(err) {
        os.Mkdir(pathName, os.ModePerm)
    }
    return path.Join(pathName, fmt.Sprintf("%05d", splitId))
}

func (e *_Environment) start() {
    if e.started {
        return
    }

    if _, err := os.Stat(e.goparkWorkDir); os.IsNotExist(err) {
        os.Mkdir(e.goparkWorkDir, os.ModePerm)
    }

    // create sub job working dir
    pathName := fmt.Sprintf("gopark-%s-%s-%d", e.master, time.Now().Format("20060102150405"), os.Getpid())
    e.jobWorkDir = path.Join(e.goparkWorkDir, pathName)
    if _, err := os.Stat(e.jobWorkDir); os.IsNotExist(err) {
        os.Mkdir(e.jobWorkDir, os.ModePerm)
    }

    // need to setup the basic tracker servers and etc.

    e.started = true
}

func (e *_Environment) stop() {
    if !e.started {
        return
    }

    e.started = false
}

var env *_Environment

func init() {
    env = &_Environment{}
}

func parklog(fmt string, v ...interface{}) {
    if env.verbose {
        log.Printf(fmt, v...)
    }
}

func ParseOptions() {
    flag.StringVar(&env.master, "master", "local", "Master of Gpark: local")
    flag.IntVar(&env.parallel, "p", 2, "Number of parallelism level, must >= 0")
    flag.StringVar(&env.goparkWorkDir, "workdir", "/opt/tmp", "Working Directory of Gpark")
    flag.BoolVar(&env.verbose, "v", false, "Output verbose log information.")

    flag.Parse()

    if env.parallel < 0 {
        flag.Usage()
        os.Exit(1)
    }
}
