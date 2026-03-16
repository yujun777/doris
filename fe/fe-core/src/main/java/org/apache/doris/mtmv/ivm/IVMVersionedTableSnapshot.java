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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;

import java.util.Optional;

/** Concrete table snapshot used by IVM once a stream snapshot has been fully resolved in FE. */
public class IVMVersionedTableSnapshot implements IVMTableSnapshot {
    private final Optional<TableSnapshot> tableSnapshot;
    private final Optional<MvccSnapshot> mvccSnapshot;
    private final Optional<MTMVSnapshotIf> mtmvSnapshot;

    public IVMVersionedTableSnapshot(Optional<TableSnapshot> tableSnapshot,
            Optional<MvccSnapshot> mvccSnapshot, Optional<MTMVSnapshotIf> mtmvSnapshot) {
        this.tableSnapshot = tableSnapshot;
        this.mvccSnapshot = mvccSnapshot;
        this.mtmvSnapshot = mtmvSnapshot;
    }

    @Override
    public Optional<TableSnapshot> asTableSnapshot() {
        return tableSnapshot;
    }

    @Override
    public Optional<MvccSnapshot> asMvccSnapshot() {
        return mvccSnapshot;
    }

    @Override
    public Optional<MTMVSnapshotIf> asMtmvSnapshot() {
        return mtmvSnapshot;
    }
}
