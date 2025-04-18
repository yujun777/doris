# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

DIR=$(
    cd $(dirname $0)
    pwd
)

source $DIR/common.sh

REGISTER_FILE="${DORIS_HOME}/status/fe-${MY_ID}-register"
MASTER_EDITLOG_PORT=""

add_local_fe() {
    wait_master_fe_ready

    while true; do
        output=$(mysql -P $MASTER_FE_PORT -h $MASTER_FE_IP -u root --execute "ALTER SYSTEM ADD FOLLOWER '$MY_IP:$MY_EDITLOG_PORT';" 2>&1)
        res=$?
        health_log "${output}\n"
        [ $res -eq 0 ] && break
        (echo $output | grep "frontend already exists") && break
        sleep 1
    done

    touch $REGISTER_FILE
}

fe_daemon() {
    set +e
    while true; do
        sleep 1
        output=$(mysql -P $MY_QUERY_PORT -h $MY_IP -u root --execute "SHOW FRONTENDS;")
        code=$?
        if [ $code -ne 0 ]; then
            health_log "daemon get frontends exec show frontends bad: $output"
            continue
        fi
        header=$(grep IsMaster <<<$output)
        if [ $? -ne 0 ]; then
            health_log "not found header"
            continue
        fi
        host_index=-1
        is_master_index=-1
        query_port_index=-1
        i=1
        for field in $header; do
            [[ "$field" = "Host" ]] && host_index=$i
            [[ "$field" = "IsMaster" ]] && is_master_index=$i
            [[ "$field" = "QueryPort" ]] && query_port_index=$i
            ((i = i + 1))
        done
        if [ $host_index -eq -1 ]; then
            health_log "header not found Host"
            continue
        fi
        if [ $is_master_index -eq -1 ]; then
            health_log "header not found IsMaster"
            continue
        fi
        if [ $query_port_index -eq -1 ]; then
            health_log "header not found QueryPort"
            continue
        fi
        echo "$output" | awk -v query_port="$query_port_index" -v is_master="$is_master_index" -v host="$host_index" '{print $query_port $is_master $host}' | grep $MY_QUERY_PORT | grep $MY_IP | grep true 2>&1
        if [ $? -eq 0 ]; then
            echo ${MY_IP}:${MY_QUERY_PORT} >$MASTER_FE_QUERY_ADDR_FILE
            if [ "$MASTER_FE_IP" != "$MY_IP" -o "${MASTER_FE_PORT}" != "${MY_QUERY_PORT}" ]; then
                health_log "change to master, last master is ${MASTER_FE_IP}:${MASTER_FE_PORT}"
                MASTER_FE_IP=$MY_IP
                MASTER_FE_PORT=$MY_QUERY_PORT
            fi
        fi
    done
}

run_fe() {
    health_log "run start_fe.sh"
    bash $DORIS_HOME/bin/start_fe.sh --daemon $@ | tee -a $DORIS_HOME/log/fe.out
}

start_cloud_fe() {
    if [ -f "$REGISTER_FILE" ]; then
        fe_daemon &
        run_fe
        return
    fi

    # Check if SQL_MODE_NODE_MGR is set to 1
    if [ "${SQL_MODE_NODE_MGR}" = "1" ]; then
        health_log "SQL_MODE_NODE_MGR is set to 1. Skipping add FE."

        touch $REGISTER_FILE

        fe_daemon &
        run_fe

        return
    fi

    wait_create_instance

    action=add_cluster
    node_type=FE_MASTER
    if [ "$MY_ID" != "1" ]; then
        wait_master_fe_ready
        action=add_node
        node_type=FE_OBSERVER
    fi

    if [ "a$IS_FE_FOLLOWER" == "a1" ]; then
        node_type=FE_FOLLOWER
    fi

    nodes='{
        "cloud_unique_id": "'"${CLOUD_UNIQUE_ID}"'",
        "ip": "'"${MY_IP}"'",
        "edit_log_port": "'"${MY_EDITLOG_PORT}"'",
        "node_type": "'"${node_type}"'"
    }'

    lock_cluster

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/${action}?token=greedisgood9999" \
        -d '{"instance_id": "default_instance_id",
        "cluster": {
            "type": "SQL",
            "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
            "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER",
            "nodes": ['"${nodes}"']
        }}')

    unlock_cluster

    health_log "add cluster. output: $output"
    code=$(jq -r '.code' <<<$output)

    if [ "$code" != "OK" ]; then
        health_log "add cluster failed,  exit."
        exit 1
    fi

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/get_cluster?token=greedisgood9999" \
        -d '{"instance_id": "default_instance_id",
            "cloud_unique_id": "'"${CLOUD_UNIQUE_ID}"'",
            "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
            "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"}')

    health_log "get cluster is: $output"
    code=$(jq -r '.code' <<<$output)

    if [ "$code" != "OK" ]; then
        health_log "get cluster failed,  exit."
        exit 1
    fi

    touch $REGISTER_FILE

    fe_daemon &
    run_fe
}

stop_frontend() {
    if [ "$STOP_GRACE" = "1" ]; then
        bash $DORIS_HOME/bin/stop_fe.sh --grace
    else
        bash $DORIS_HOME/bin/stop_fe.sh
    fi

    exit 0
}

wait_process() {
    pid=""
    for ((i = 0; i < 5; i++)); do
        sleep 1s
        pid=$(ps -elf | grep java | grep org.apache.doris.DorisFE | grep -v grep | awk '{print $4}')
        if [ -n "$pid" ]; then
            break
        fi
    done

    wait_pid $pid
}

fetch_master_fe_editlog_port() {
    set +e
    while true; do
        output=$(mysql -P $MY_QUERY_PORT -h $MASTER_FE_IP -u root -N -s --execute "select EditLogPort from frontends() where IsMaster = 'true';")
        code=$?
        if [ $code -ne 0 ]; then
            health_log "get editlog port exec show frontends bad: $output"
            sleep 1
            continue
        fi
        if [ -n "$output" ]; then
            MASTER_EDITLOG_PORT=${output}
            break
        fi
    done
}

start_local_fe() {
    if [ "$MY_ID" = "1" -a ! -f $REGISTER_FILE ]; then
        touch $REGISTER_FILE
    fi

    if [ -f $REGISTER_FILE ]; then
        fe_daemon &
        run_fe
    else
        add_local_fe
        fe_daemon &
        fetch_master_fe_editlog_port
        run_fe --helper $MASTER_FE_IP:$MASTER_EDITLOG_PORT
    fi
}

main() {
    trap stop_frontend SIGTERM

    if [ "$IS_CLOUD" == "1" ]; then
        start_cloud_fe
    else
        start_local_fe
    fi

    wait_process
}

main
