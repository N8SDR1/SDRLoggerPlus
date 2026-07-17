// Maps the GeoJSON feature `name` (see naAdmin1.geo.json) to the short prefix /
// abbreviation shown on the contest geo map. US and Canadian entities use the
// abbreviations amateurs use as QSO-party / ARRL-section multipliers (standard
// USPS state codes and RAC provincial codes). Mexican states use ISO 3166-2
// short codes for a compact label; ham call districts are XE1/XE2/XE3.

export type CountryCode = 'US' | 'CA' | 'MX';

/** name (as it appears in the GeoJSON) -> prefix/abbreviation */
export const PREFIX_BY_NAME: Record<string, string> = {
  // United States (+ DC, PR)
  Alabama: 'AL', Alaska: 'AK', Arizona: 'AZ', Arkansas: 'AR', California: 'CA',
  Colorado: 'CO', Connecticut: 'CT', Delaware: 'DE', 'District of Columbia': 'DC',
  Florida: 'FL', Georgia: 'GA', Hawaii: 'HI', Idaho: 'ID', Illinois: 'IL',
  Indiana: 'IN', Iowa: 'IA', Kansas: 'KS', Kentucky: 'KY', Louisiana: 'LA',
  Maine: 'ME', Maryland: 'MD', Massachusetts: 'MA', Michigan: 'MI', Minnesota: 'MN',
  Mississippi: 'MS', Missouri: 'MO', Montana: 'MT', Nebraska: 'NE', Nevada: 'NV',
  'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
  'North Carolina': 'NC', 'North Dakota': 'ND', Ohio: 'OH', Oklahoma: 'OK',
  Oregon: 'OR', Pennsylvania: 'PA', 'Puerto Rico': 'PR', 'Rhode Island': 'RI',
  'South Carolina': 'SC', 'South Dakota': 'SD', Tennessee: 'TN', Texas: 'TX',
  Utah: 'UT', Vermont: 'VT', Virginia: 'VA', Washington: 'WA',
  'West Virginia': 'WV', Wisconsin: 'WI', Wyoming: 'WY',

  // Canada (RAC provincial codes)
  Alberta: 'AB', 'British Columbia': 'BC', Manitoba: 'MB', 'New Brunswick': 'NB',
  'Newfoundland and Labrador': 'NL', 'Northwest Territories': 'NT',
  'Nova Scotia': 'NS', Nunavut: 'NU', Ontario: 'ON', 'Prince Edward Island': 'PE',
  Quebec: 'QC', Saskatchewan: 'SK', 'Yukon Territory': 'YT',

  // Mexico (ISO 3166-2:MX short codes)
  Aguascalientes: 'AGU', 'Baja California': 'BCN', 'Baja California Sur': 'BCS',
  Campeche: 'CAM', Chiapas: 'CHP', Chihuahua: 'CHH', 'Coahuila de Zaragoza': 'COA',
  Colima: 'COL', 'Distrito Federal': 'CMX', Durango: 'DUR', Guanajuato: 'GUA',
  Guerrero: 'GRO', Hidalgo: 'HID', Jalisco: 'JAL', 'Michoacán de Ocampo': 'MIC',
  Morelos: 'MOR', 'México': 'MEX', Nayarit: 'NAY', 'Nuevo León': 'NLE',
  Oaxaca: 'OAX', Puebla: 'PUE', 'Querétaro': 'QUE', 'Quintana Roo': 'ROO',
  'San Luis Potosí': 'SLP', Sinaloa: 'SIN', Sonora: 'SON', Tabasco: 'TAB',
  Tamaulipas: 'TAM', Tlaxcala: 'TLA', 'Veracruz de Ignacio de la Llave': 'VER',
  'Yucatán': 'YUC', Zacatecas: 'ZAC',
};

/** Short display name (trim the long official Mexican suffixes for the readout). */
export const SHORT_NAME_BY_NAME: Record<string, string> = {
  'Coahuila de Zaragoza': 'Coahuila',
  'Michoacán de Ocampo': 'Michoacán',
  'Veracruz de Ignacio de la Llave': 'Veracruz',
  'Distrito Federal': 'Ciudad de México',
};

export const COUNTRY_COLORS: Record<CountryCode, string> = {
  US: '#00ddff', // cyan (app accent)
  CA: '#ffb432', // amber
  MX: '#10b981', // green
};

export const COUNTRY_NAMES: Record<CountryCode, string> = {
  US: 'United States',
  CA: 'Canada',
  MX: 'Mexico',
};

export function getPrefix(name: string): string {
  return PREFIX_BY_NAME[name] ?? '';
}

export function getShortName(name: string): string {
  return SHORT_NAME_BY_NAME[name] ?? name;
}
