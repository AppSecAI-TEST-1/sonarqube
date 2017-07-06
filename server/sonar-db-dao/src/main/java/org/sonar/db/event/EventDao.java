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
package org.sonar.db.event;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.sonar.core.util.stream.MoreCollectors;
import org.sonar.db.Dao;
import org.sonar.db.DbSession;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.sonar.db.DatabaseUtils.executeLargeInputs;

public class EventDao implements Dao {

  public Optional<EventDto> selectByUuid(DbSession dbSession, String uuid) {
    return Optional.ofNullable(mapper(dbSession).selectByUuid(uuid));
  }

  public List<EventDto> selectByComponentUuid(DbSession session, String componentUuid) {
    return session.getMapper(EventMapper.class).selectByComponentUuid(componentUuid);
  }

  public List<EventDto> selectByAnalysisUuid(DbSession dbSession, String uuid) {
    return mapper(dbSession).selectByAnalysisUuid(uuid);
  }

  public List<EventDto> selectByAnalysisUuids(DbSession dbSession, List<String> analyses) {
    return executeLargeInputs(analyses, mapper(dbSession)::selectByAnalysisUuids);
  }

  public List<EventDto> selectByComponentUuidsAndFromDates(DbSession dbSession, List<String> componentUuids, List<Long> fromDates) {
    checkArgument(componentUuids.size() == fromDates.size(), "The number of components (%s) and from dates (%s) must be the same.",
      String.valueOf(componentUuids.size()),
      String.valueOf(fromDates.size()));
    List<ComponentUuidFromDatePair> componentUuidFromDatePairs = IntStream.range(0, componentUuids.size())
      .mapToObj(i -> new ComponentUuidFromDatePair(componentUuids.get(i), fromDates.get(i)))
      .collect(MoreCollectors.toList(componentUuids.size()));
    return executeLargeInputs(componentUuidFromDatePairs, partition -> mapper(dbSession).selectByQuery(partition), i -> i / 2);
  }

  public EventDto insert(DbSession session, EventDto dto) {
    session.getMapper(EventMapper.class).insert(dto);

    return dto;
  }

  public void update(DbSession dbSession, String uuid, @Nullable String name, @Nullable String description) {
    mapper(dbSession).update(uuid, name, description);
  }

  public void delete(DbSession session, Long id) {
    mapper(session).deleteById(id);
  }

  public void delete(DbSession session, String uuid) {
    mapper(session).deleteByUuid(uuid);
  }

  private static EventMapper mapper(DbSession session) {
    return session.getMapper(EventMapper.class);
  }

  static class ComponentUuidFromDatePair implements Comparable<ComponentUuidFromDatePair> {
    private final String componentUuid;
    private final long from;

    ComponentUuidFromDatePair(String componentUuid, long from) {
      this.componentUuid = requireNonNull(componentUuid);
      this.from = from;
    }

    @Override
    public int compareTo(ComponentUuidFromDatePair other) {
      if (this == other) {
        return 0;
      }

      int c = componentUuid.compareTo(other.componentUuid);
      if (c == 0) {
        c = Long.compare(from, other.from);
      }

      return c;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ComponentUuidFromDatePair other = (ComponentUuidFromDatePair) o;
      return componentUuid.equals(other.componentUuid)
      && from == other.from;
    }

    @Override
    public int hashCode() {
      return Objects.hash(componentUuid, from);
    }
  }
}
