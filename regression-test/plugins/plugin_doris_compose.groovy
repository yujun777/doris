// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.doris.regression.suite.Suite

import groovy.json.JsonSlurper

Suite.metaClass.newCluster = { String cluster, String image, int feNum = 1, int beNum = 3, int timeout = 180 ->
    assert cluster != null && cluster != ""
    assert image != null && image != ""
    assert feNum > 0
    assert beNum > 0
    destroyCluster(cluster)

    def cmd = String.format("up %s %s --add-fe-num %s --add-be-num %s --wait-timeout %s",
            cluster, image, feNum, beNum, timeout)
    def data = runDorisCompose(cmd)

    sleep(5000)

    return data
}

Suite.metaClass.destroyCluster = { String cluster, boolean deleteFiles = true ->
    def cmd = "down " + cluster
    if (deleteFiles) {
        cmd += " --clean"
    }
    runDorisCompose(cmd)
}

Suite.metaClass.runDorisCompose = { String cmd, int timeout = -1 ->
    def fullCmd = String.format("python  %s  %s  --output-json", context.config.dorisComposePath, cmd)
    logger.info("Run doris compose cmd: " + fullCmd)
    def proc = fullCmd.execute()
    def outBuf = new StringBuilder()
    def errBuf = new StringBuilder()
    proc.consumeProcessOutput(outBuf, errBuf)
    if (timeout > 0) {
        proc.waitForOrKill(timeout)
    } else {
        proc.waitFor()
    }
    def out = outBuf.toString()
    def err = errBuf.toString()
    if (proc.exitValue()) {
        throw new Exception(String.format("Exit value: %s != 0, stdout: %s, stderr: %s",
                proc.exitValue(), out, err))
    }
    def parser = new JsonSlurper()
    def object = parser.parseText(out)
    assert object instanceof Map
    if (object.code != 0) {
        throw new Exception(String.format("Code: %s != 0, err: %s", object.code, object.err))
    }
    return object.data
}

logger.info("Added doris compose functions to Suite")
