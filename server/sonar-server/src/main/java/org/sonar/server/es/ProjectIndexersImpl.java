/*
 * SonarQube
 * Copyright (C) 2009-2017 SonarSource SA
 * mailto:info AT sonarsource DOT com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.sonar.server.es;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.sonar.db.DbSession;
import org.sonar.db.es.EsQueueDto;

import static java.util.Arrays.asList;

public class ProjectIndexersImpl implements ProjectIndexers {

  private final List<ProjectIndexer> indexers;

  public ProjectIndexersImpl(ProjectIndexer... indexers) {
    this.indexers = asList(indexers);
  }

  @Override
  public void commitAndIndex(DbSession dbSession, Collection<String> projectUuids, ProjectIndexer.Cause cause) {
    List<EsQueueDto> items = new ArrayList<>();
    indexers.forEach(i -> items.addAll(i.prepareForRecovery(dbSession, projectUuids, cause)));
    dbSession.commit();
    indexers.forEach(i -> i.index(dbSession, items));
  }
}
