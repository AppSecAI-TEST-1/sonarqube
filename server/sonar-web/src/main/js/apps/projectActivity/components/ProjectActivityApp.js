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
// @flow
import React from 'react';
import Helmet from 'react-helmet';
import moment from 'moment';
import ProjectActivityPageHeader from './ProjectActivityPageHeader';
import ProjectActivityAnalysesList from './ProjectActivityAnalysesList';
import ProjectActivityGraphs from './ProjectActivityGraphs';
import { getDisplayedHistoryMetrics } from '../utils';
import { translate } from '../../../helpers/l10n';
import './projectActivity.css';
import type { Analysis, MeasureHistory, Metric, Query } from '../types';

type Props = {
  addCustomEvent: (analysis: string, name: string, category?: string) => Promise<*>,
  addVersion: (analysis: string, version: string) => Promise<*>,
  analyses: Array<Analysis>,
  analysesLoading: boolean,
  changeEvent: (event: string, name: string) => Promise<*>,
  deleteAnalysis: (analysis: string) => Promise<*>,
  deleteEvent: (analysis: string, event: string) => Promise<*>,
  graphLoading: boolean,
  loading: boolean,
  project: { configuration?: { showHistory: boolean }, key: string, leakPeriodDate: string },
  metrics: Array<Metric>,
  measuresHistory: Array<MeasureHistory>,
  query: Query,
  updateQuery: (newQuery: Query) => void
};

export default class ProjectActivityApp extends React.PureComponent {
  props: Props;

  getMetricType = () => {
    const historyMetrics = getDisplayedHistoryMetrics(
      this.props.query.graph,
      this.props.query.customMetrics
    );
    const metricKey = historyMetrics.length > 0 ? historyMetrics[0] : '';
    const metric = this.props.metrics.find(metric => metric.key === metricKey);
    return metric ? metric.type : 'INT';
  };

  render() {
    const { analyses, measuresHistory, query } = this.props;
    const { configuration } = this.props.project;
    const canAdmin = configuration ? configuration.showHistory : false;
    return (
      <div id="project-activity" className="page page-limited">
        <Helmet title={translate('project_activity.page')} />

        <ProjectActivityPageHeader
          category={query.category}
          from={query.from}
          to={query.to}
          updateQuery={this.props.updateQuery}
        />

        <div className="layout-page project-activity-page">
          <div className="layout-page-side-outer project-activity-page-side-outer boxed-group">
            <ProjectActivityAnalysesList
              addCustomEvent={this.props.addCustomEvent}
              addVersion={this.props.addVersion}
              analysesLoading={this.props.analysesLoading}
              analyses={analyses}
              canAdmin={canAdmin}
              className="boxed-group-inner"
              changeEvent={this.props.changeEvent}
              deleteAnalysis={this.props.deleteAnalysis}
              deleteEvent={this.props.deleteEvent}
              loading={this.props.loading}
              query={this.props.query}
              updateQuery={this.props.updateQuery}
            />
          </div>
          <div className="project-activity-layout-page-main">
            <ProjectActivityGraphs
              analyses={analyses}
              leakPeriodDate={moment(this.props.project.leakPeriodDate).toDate()}
              loading={this.props.graphLoading}
              measuresHistory={measuresHistory}
              metrics={this.props.metrics}
              metricsType={this.getMetricType()}
              query={query}
              updateQuery={this.props.updateQuery}
            />
          </div>
        </div>
      </div>
    );
  }
}
