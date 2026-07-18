import type { IJsonModel } from 'flexlayout-react';

// Built-in panel arrangements, one per operating style. They ship with the app
// and are read-only: applying one swaps the panels, but it can't be renamed or
// deleted and it doesn't consume one of the three user preset slots. Named to
// match the Log Entry modes (General / POTA / SAT) plus Contest, since those are
// the activities an operator actually switches between.

type Tab = [component: string, name: string];
interface Column {
  weight: number;
  tabs: Tab[];
}

// Share of the window height given to the header strip. Matches what operators
// actually drag it to; the app's defaultLayout still uses 6, which renders the
// bar too short to read.
const HEADER_WEIGHT = 10;

// Every starter shares the same skeleton: the header strip across the top, then
// a row of tabsets beneath it. Keeps them consistent and easy to tweak.
function build(columns: Column[]): IJsonModel {
  return {
    global: {
      tabEnableFloat: false,
      tabSetMinWidth: 100,
      tabSetMinHeight: 40,
      borderMinSize: 100,
    },
    borders: [],
    layout: {
      type: 'row',
      weight: 100,
      children: [
        {
          type: 'row',
          weight: 100,
          children: [
            {
              // 10/90, not the 6/94 the app's defaultLayout uses — at 6% the
              // header strip is too short to read its contents.
              type: 'tabset',
              weight: HEADER_WEIGHT,
              children: [
                { type: 'tab', name: 'Header Bar', component: 'header-bar', enableClose: false },
              ],
            },
            {
              type: 'row',
              weight: 100 - HEADER_WEIGHT,
              children: columns.map((col) => ({
                type: 'tabset',
                weight: col.weight,
                children: col.tabs.map(([component, name]) => ({ type: 'tab', name, component })),
              })),
            },
          ],
        },
      ],
    },
  };
}

export interface StarterLayout {
  name: string;
  description: string;
  layout: IJsonModel;
}

export const STARTER_LAYOUTS: StarterLayout[] = [
  {
    name: 'General',
    description: 'Everyday logging — entry, history, cluster and callbook',
    layout: build([
      { weight: 30, tabs: [['log-entry', 'Log Entry'], ['qrz-profile', 'QRZ Profile']] },
      { weight: 42, tabs: [['log-history', 'Log History'], ['meters', 'Meters']] },
      { weight: 28, tabs: [['cluster', 'DX Cluster'], ['propagation', 'Propagation']] },
    ]),
  },
  {
    name: 'Contest',
    description: 'Running a contest — entry, score, bandmap and multipliers',
    layout: build([
      { weight: 32, tabs: [['contest-entry', 'Contest Entry'], ['contest-score', 'Contest Score']] },
      { weight: 40, tabs: [['contest-bandmap', 'Bandmap'], ['log-history', 'Log History']] },
      { weight: 28, tabs: [['contest-mults', 'Multipliers'], ['contests', 'Contests']] },
    ]),
  },
  {
    name: 'POTA',
    description: 'Parks activation — entry, park info and hunting',
    layout: build([
      { weight: 32, tabs: [['log-entry', 'Log Entry'], ['pota', 'POTA']] },
      { weight: 40, tabs: [['log-history', 'Log History'], ['map', '2D Map']] },
      { weight: 28, tabs: [['cluster', 'DX Cluster'], ['propagation', 'Propagation']] },
    ]),
  },
  {
    name: 'Satellite',
    description: 'Working birds — pass control, rotator and entry',
    layout: build([
      { weight: 32, tabs: [['log-entry', 'Log Entry'], ['sat-controller', 'SAT Controller']] },
      { weight: 40, tabs: [['rotator', 'Rotator'], ['log-history', 'Log History']] },
      { weight: 28, tabs: [['globe-3d', '3D Globe'], ['grid-tracker', 'Grid Tracker']] },
    ]),
  },
  {
    name: 'Digital',
    description: 'FT8 and friends — decodes, entry and grid chasing',
    layout: build([
      { weight: 32, tabs: [['log-entry', 'Log Entry'], ['qrz-profile', 'QRZ Profile']] },
      { weight: 40, tabs: [['wsjtx-decodes', 'Digital Decodes'], ['log-history', 'Log History']] },
      { weight: 28, tabs: [['grid-tracker', 'Grid Tracker'], ['cluster', 'DX Cluster']] },
    ]),
  },
];

export function findStarterLayout(name: string): StarterLayout | undefined {
  return STARTER_LAYOUTS.find((l) => l.name === name);
}
