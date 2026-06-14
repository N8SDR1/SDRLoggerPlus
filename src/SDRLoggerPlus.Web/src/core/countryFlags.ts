// Country flags mapping for amateur radio logging
// Uses emoji flags which are universally supported
// Country names match DXCC entity names commonly used in amateur radio

export interface CountryFlag {
  code: string;      // ISO 3166-1 alpha-2 code
  flag: string;      // Emoji flag
  name: string;      // Country name (DXCC style)
  aliases?: string[]; // Alternative names
}

// Comprehensive list of countries with their flags
// Sorted alphabetically by name for easy lookup
export const COUNTRY_FLAGS: CountryFlag[] = [
  { code: 'AF', flag: '🇦🇫', name: 'Afghanistan' },
  { code: 'AL', flag: '🇦🇱', name: 'Albania' },
  { code: 'DZ', flag: '🇩🇿', name: 'Algeria' },
  { code: 'AD', flag: '🇦🇩', name: 'Andorra' },
  { code: 'AO', flag: '🇦🇴', name: 'Angola' },
  { code: 'AG', flag: '🇦🇬', name: 'Antigua & Barbuda', aliases: ['Antigua and Barbuda'] },
  { code: 'AR', flag: '🇦🇷', name: 'Argentina' },
  { code: 'AM', flag: '🇦🇲', name: 'Armenia' },
  { code: 'AU', flag: '🇦🇺', name: 'Australia' },
  { code: 'AT', flag: '🇦🇹', name: 'Austria' },
  { code: 'AZ', flag: '🇦🇿', name: 'Azerbaijan' },
  { code: 'BS', flag: '🇧🇸', name: 'Bahamas' },
  { code: 'BH', flag: '🇧🇭', name: 'Bahrain' },
  { code: 'BD', flag: '🇧🇩', name: 'Bangladesh' },
  { code: 'BB', flag: '🇧🇧', name: 'Barbados' },
  { code: 'BY', flag: '🇧🇾', name: 'Belarus' },
  { code: 'BE', flag: '🇧🇪', name: 'Belgium' },
  { code: 'BZ', flag: '🇧🇿', name: 'Belize' },
  { code: 'BJ', flag: '🇧🇯', name: 'Benin' },
  { code: 'BT', flag: '🇧🇹', name: 'Bhutan' },
  { code: 'BO', flag: '🇧🇴', name: 'Bolivia' },
  { code: 'BA', flag: '🇧🇦', name: 'Bosnia-Herzegovina', aliases: ['Bosnia and Herzegovina', 'Bosnia'] },
  { code: 'BW', flag: '🇧🇼', name: 'Botswana' },
  { code: 'BR', flag: '🇧🇷', name: 'Brazil' },
  { code: 'BN', flag: '🇧🇳', name: 'Brunei' },
  { code: 'BG', flag: '🇧🇬', name: 'Bulgaria' },
  { code: 'BF', flag: '🇧🇫', name: 'Burkina Faso' },
  { code: 'BI', flag: '🇧🇮', name: 'Burundi' },
  { code: 'KH', flag: '🇰🇭', name: 'Cambodia' },
  { code: 'CM', flag: '🇨🇲', name: 'Cameroon' },
  { code: 'CA', flag: '🇨🇦', name: 'Canada' },
  { code: 'CV', flag: '🇨🇻', name: 'Cape Verde' },
  { code: 'CF', flag: '🇨🇫', name: 'Central African Republic' },
  { code: 'TD', flag: '🇹🇩', name: 'Chad' },
  { code: 'CL', flag: '🇨🇱', name: 'Chile' },
  { code: 'CN', flag: '🇨🇳', name: 'China', aliases: ["People's Republic of China"] },
  { code: 'CO', flag: '🇨🇴', name: 'Colombia' },
  { code: 'KM', flag: '🇰🇲', name: 'Comoros' },
  { code: 'CG', flag: '🇨🇬', name: 'Congo', aliases: ['Republic of the Congo'] },
  { code: 'CD', flag: '🇨🇩', name: 'Dem. Rep. of the Congo', aliases: ['Democratic Republic of the Congo', 'DRC', 'Congo (DRC)'] },
  { code: 'CR', flag: '🇨🇷', name: 'Costa Rica' },
  { code: 'HR', flag: '🇭🇷', name: 'Croatia' },
  { code: 'CU', flag: '🇨🇺', name: 'Cuba' },
  { code: 'CY', flag: '🇨🇾', name: 'Cyprus' },
  { code: 'CZ', flag: '🇨🇿', name: 'Czech Republic', aliases: ['Czechia'] },
  { code: 'DK', flag: '🇩🇰', name: 'Denmark' },
  { code: 'DJ', flag: '🇩🇯', name: 'Djibouti' },
  { code: 'DM', flag: '🇩🇲', name: 'Dominica' },
  { code: 'DO', flag: '🇩🇴', name: 'Dominican Republic' },
  { code: 'TL', flag: '🇹🇱', name: 'East Timor', aliases: ['Timor-Leste'] },
  { code: 'EC', flag: '🇪🇨', name: 'Ecuador' },
  { code: 'EG', flag: '🇪🇬', name: 'Egypt' },
  { code: 'SV', flag: '🇸🇻', name: 'El Salvador' },
  { code: 'GQ', flag: '🇬🇶', name: 'Equatorial Guinea' },
  { code: 'ER', flag: '🇪🇷', name: 'Eritrea' },
  { code: 'EE', flag: '🇪🇪', name: 'Estonia' },
  { code: 'SZ', flag: '🇸🇿', name: 'Eswatini', aliases: ['Swaziland'] },
  { code: 'ET', flag: '🇪🇹', name: 'Ethiopia' },
  { code: 'FJ', flag: '🇫🇯', name: 'Fiji' },
  { code: 'FI', flag: '🇫🇮', name: 'Finland' },
  { code: 'FR', flag: '🇫🇷', name: 'France' },
  { code: 'GA', flag: '🇬🇦', name: 'Gabon' },
  { code: 'GM', flag: '🇬🇲', name: 'Gambia', aliases: ['The Gambia'] },
  { code: 'GE', flag: '🇬🇪', name: 'Georgia' },
  { code: 'DE', flag: '🇩🇪', name: 'Germany', aliases: ['Fed. Rep. of Germany', 'Federal Republic of Germany'] },
  { code: 'GH', flag: '🇬🇭', name: 'Ghana' },
  { code: 'GR', flag: '🇬🇷', name: 'Greece' },
  { code: 'GD', flag: '🇬🇩', name: 'Grenada' },
  { code: 'GT', flag: '🇬🇹', name: 'Guatemala' },
  { code: 'GN', flag: '🇬🇳', name: 'Guinea' },
  { code: 'GW', flag: '🇬🇼', name: 'Guinea-Bissau' },
  { code: 'GY', flag: '🇬🇾', name: 'Guyana' },
  { code: 'HT', flag: '🇭🇹', name: 'Haiti' },
  { code: 'HN', flag: '🇭🇳', name: 'Honduras' },
  { code: 'HU', flag: '🇭🇺', name: 'Hungary' },
  { code: 'IS', flag: '🇮🇸', name: 'Iceland' },
  { code: 'IN', flag: '🇮🇳', name: 'India' },
  { code: 'ID', flag: '🇮🇩', name: 'Indonesia' },
  { code: 'IR', flag: '🇮🇷', name: 'Iran' },
  { code: 'IQ', flag: '🇮🇶', name: 'Iraq' },
  { code: 'IE', flag: '🇮🇪', name: 'Ireland', aliases: ['Republic of Ireland', 'Éire'] },
  { code: 'IL', flag: '🇮🇱', name: 'Israel' },
  { code: 'IT', flag: '🇮🇹', name: 'Italy' },
  { code: 'CI', flag: '🇨🇮', name: 'Ivory Coast', aliases: ["Côte d'Ivoire", 'Cote d\'Ivoire'] },
  { code: 'JM', flag: '🇯🇲', name: 'Jamaica' },
  { code: 'JP', flag: '🇯🇵', name: 'Japan' },
  { code: 'JO', flag: '🇯🇴', name: 'Jordan' },
  { code: 'KZ', flag: '🇰🇿', name: 'Kazakhstan' },
  { code: 'KE', flag: '🇰🇪', name: 'Kenya' },
  { code: 'KI', flag: '🇰🇮', name: 'Kiribati' },
  { code: 'XK', flag: '🇽🇰', name: 'Kosovo' },
  { code: 'KW', flag: '🇰🇼', name: 'Kuwait' },
  { code: 'KG', flag: '🇰🇬', name: 'Kyrgyzstan' },
  { code: 'LA', flag: '🇱🇦', name: 'Laos' },
  { code: 'LV', flag: '🇱🇻', name: 'Latvia' },
  { code: 'LB', flag: '🇱🇧', name: 'Lebanon' },
  { code: 'LS', flag: '🇱🇸', name: 'Lesotho' },
  { code: 'LR', flag: '🇱🇷', name: 'Liberia' },
  { code: 'LY', flag: '🇱🇾', name: 'Libya' },
  { code: 'LI', flag: '🇱🇮', name: 'Liechtenstein' },
  { code: 'LT', flag: '🇱🇹', name: 'Lithuania' },
  { code: 'LU', flag: '🇱🇺', name: 'Luxembourg' },
  { code: 'MG', flag: '🇲🇬', name: 'Madagascar' },
  { code: 'MW', flag: '🇲🇼', name: 'Malawi' },
  { code: 'MY', flag: '🇲🇾', name: 'Malaysia', aliases: ['West Malaysia', 'East Malaysia'] },
  { code: 'MV', flag: '🇲🇻', name: 'Maldives' },
  { code: 'ML', flag: '🇲🇱', name: 'Mali' },
  { code: 'MT', flag: '🇲🇹', name: 'Malta' },
  { code: 'MH', flag: '🇲🇭', name: 'Marshall Islands' },
  { code: 'MR', flag: '🇲🇷', name: 'Mauritania' },
  { code: 'MU', flag: '🇲🇺', name: 'Mauritius' },
  { code: 'MX', flag: '🇲🇽', name: 'Mexico' },
  { code: 'FM', flag: '🇫🇲', name: 'Micronesia' },
  { code: 'MD', flag: '🇲🇩', name: 'Moldova' },
  { code: 'MC', flag: '🇲🇨', name: 'Monaco' },
  { code: 'MN', flag: '🇲🇳', name: 'Mongolia' },
  { code: 'ME', flag: '🇲🇪', name: 'Montenegro' },
  { code: 'MA', flag: '🇲🇦', name: 'Morocco' },
  { code: 'MZ', flag: '🇲🇿', name: 'Mozambique' },
  { code: 'MM', flag: '🇲🇲', name: 'Myanmar', aliases: ['Burma'] },
  { code: 'NA', flag: '🇳🇦', name: 'Namibia' },
  { code: 'NR', flag: '🇳🇷', name: 'Nauru' },
  { code: 'NP', flag: '🇳🇵', name: 'Nepal' },
  { code: 'NL', flag: '🇳🇱', name: 'Netherlands', aliases: ['Holland'] },
  { code: 'NZ', flag: '🇳🇿', name: 'New Zealand' },
  { code: 'NI', flag: '🇳🇮', name: 'Nicaragua' },
  { code: 'NE', flag: '🇳🇪', name: 'Niger' },
  { code: 'NG', flag: '🇳🇬', name: 'Nigeria' },
  { code: 'KP', flag: '🇰🇵', name: 'North Korea', aliases: ["Democratic People's Republic of Korea", 'DPRK'] },
  { code: 'MK', flag: '🇲🇰', name: 'North Macedonia', aliases: ['Macedonia'] },
  { code: 'NO', flag: '🇳🇴', name: 'Norway' },
  { code: 'OM', flag: '🇴🇲', name: 'Oman' },
  { code: 'PK', flag: '🇵🇰', name: 'Pakistan' },
  { code: 'PW', flag: '🇵🇼', name: 'Palau' },
  { code: 'PS', flag: '🇵🇸', name: 'Palestine' },
  { code: 'PA', flag: '🇵🇦', name: 'Panama' },
  { code: 'PG', flag: '🇵🇬', name: 'Papua New Guinea' },
  { code: 'PY', flag: '🇵🇾', name: 'Paraguay' },
  { code: 'PE', flag: '🇵🇪', name: 'Peru' },
  { code: 'PH', flag: '🇵🇭', name: 'Philippines' },
  { code: 'PL', flag: '🇵🇱', name: 'Poland' },
  { code: 'PT', flag: '🇵🇹', name: 'Portugal' },
  { code: 'QA', flag: '🇶🇦', name: 'Qatar' },
  { code: 'RO', flag: '🇷🇴', name: 'Romania' },
  { code: 'RU', flag: '🇷🇺', name: 'Russia', aliases: ['Russian Federation', 'European Russia', 'Asiatic Russia'] },
  { code: 'RW', flag: '🇷🇼', name: 'Rwanda' },
  { code: 'KN', flag: '🇰🇳', name: 'Saint Kitts & Nevis', aliases: ['Saint Kitts and Nevis', 'St. Kitts & Nevis'] },
  { code: 'LC', flag: '🇱🇨', name: 'Saint Lucia', aliases: ['St. Lucia'] },
  { code: 'VC', flag: '🇻🇨', name: 'Saint Vincent', aliases: ['St. Vincent', 'Saint Vincent and the Grenadines'] },
  { code: 'WS', flag: '🇼🇸', name: 'Samoa', aliases: ['Western Samoa'] },
  { code: 'SM', flag: '🇸🇲', name: 'San Marino' },
  { code: 'ST', flag: '🇸🇹', name: 'Sao Tome & Principe', aliases: ['Sao Tome and Principe'] },
  { code: 'SA', flag: '🇸🇦', name: 'Saudi Arabia' },
  { code: 'SN', flag: '🇸🇳', name: 'Senegal' },
  { code: 'RS', flag: '🇷🇸', name: 'Serbia' },
  { code: 'SC', flag: '🇸🇨', name: 'Seychelles' },
  { code: 'SL', flag: '🇸🇱', name: 'Sierra Leone' },
  { code: 'SG', flag: '🇸🇬', name: 'Singapore' },
  { code: 'SK', flag: '🇸🇰', name: 'Slovakia' },
  { code: 'SI', flag: '🇸🇮', name: 'Slovenia' },
  { code: 'SB', flag: '🇸🇧', name: 'Solomon Islands' },
  { code: 'SO', flag: '🇸🇴', name: 'Somalia' },
  { code: 'ZA', flag: '🇿🇦', name: 'South Africa' },
  { code: 'KR', flag: '🇰🇷', name: 'South Korea', aliases: ['Republic of Korea', 'Korea'] },
  { code: 'SS', flag: '🇸🇸', name: 'South Sudan' },
  { code: 'ES', flag: '🇪🇸', name: 'Spain' },
  { code: 'LK', flag: '🇱🇰', name: 'Sri Lanka' },
  { code: 'SD', flag: '🇸🇩', name: 'Sudan' },
  { code: 'SR', flag: '🇸🇷', name: 'Suriname' },
  { code: 'SE', flag: '🇸🇪', name: 'Sweden' },
  { code: 'CH', flag: '🇨🇭', name: 'Switzerland' },
  { code: 'SY', flag: '🇸🇾', name: 'Syria' },
  { code: 'TW', flag: '🇹🇼', name: 'Taiwan' },
  { code: 'TJ', flag: '🇹🇯', name: 'Tajikistan' },
  { code: 'TZ', flag: '🇹🇿', name: 'Tanzania' },
  { code: 'TH', flag: '🇹🇭', name: 'Thailand' },
  { code: 'TG', flag: '🇹🇬', name: 'Togo' },
  { code: 'TO', flag: '🇹🇴', name: 'Tonga' },
  { code: 'TT', flag: '🇹🇹', name: 'Trinidad & Tobago', aliases: ['Trinidad and Tobago'] },
  { code: 'TN', flag: '🇹🇳', name: 'Tunisia' },
  { code: 'TR', flag: '🇹🇷', name: 'Turkey', aliases: ['Türkiye'] },
  { code: 'TM', flag: '🇹🇲', name: 'Turkmenistan' },
  { code: 'TV', flag: '🇹🇻', name: 'Tuvalu' },
  { code: 'UG', flag: '🇺🇬', name: 'Uganda' },
  { code: 'UA', flag: '🇺🇦', name: 'Ukraine' },
  { code: 'AE', flag: '🇦🇪', name: 'United Arab Emirates', aliases: ['UAE'] },
  { code: 'GB', flag: '🇬🇧', name: 'United Kingdom', aliases: ['UK', 'England', 'Scotland', 'Wales', 'Northern Ireland', 'Great Britain'] },
  { code: 'US', flag: '🇺🇸', name: 'United States', aliases: ['USA', 'United States of America', 'US', 'Alaska', 'Hawaii'] },
  { code: 'UY', flag: '🇺🇾', name: 'Uruguay' },
  { code: 'UZ', flag: '🇺🇿', name: 'Uzbekistan' },
  { code: 'VU', flag: '🇻🇺', name: 'Vanuatu' },
  { code: 'VA', flag: '🇻🇦', name: 'Vatican City', aliases: ['Vatican', 'Holy See'] },
  { code: 'VE', flag: '🇻🇪', name: 'Venezuela' },
  { code: 'VN', flag: '🇻🇳', name: 'Vietnam' },
  { code: 'YE', flag: '🇾🇪', name: 'Yemen' },
  { code: 'ZM', flag: '🇿🇲', name: 'Zambia' },
  { code: 'ZW', flag: '🇿🇼', name: 'Zimbabwe' },

  // Territories and dependencies commonly seen in amateur radio
  { code: 'AS', flag: '🇦🇸', name: 'American Samoa' },
  { code: 'AI', flag: '🇦🇮', name: 'Anguilla' },
  { code: 'AW', flag: '🇦🇼', name: 'Aruba' },
  { code: 'BM', flag: '🇧🇲', name: 'Bermuda' },
  { code: 'VG', flag: '🇻🇬', name: 'British Virgin Islands', aliases: ['Virgin Islands (British)'] },
  { code: 'KY', flag: '🇰🇾', name: 'Cayman Islands' },
  { code: 'CW', flag: '🇨🇼', name: 'Curacao', aliases: ['Curaçao'] },
  { code: 'FK', flag: '🇫🇰', name: 'Falkland Islands' },
  { code: 'FO', flag: '🇫🇴', name: 'Faroe Islands' },
  { code: 'GF', flag: '🇬🇫', name: 'French Guiana' },
  { code: 'PF', flag: '🇵🇫', name: 'French Polynesia' },
  { code: 'GI', flag: '🇬🇮', name: 'Gibraltar' },
  { code: 'GL', flag: '🇬🇱', name: 'Greenland' },
  { code: 'GP', flag: '🇬🇵', name: 'Guadeloupe' },
  { code: 'GU', flag: '🇬🇺', name: 'Guam' },
  { code: 'HK', flag: '🇭🇰', name: 'Hong Kong' },
  { code: 'MO', flag: '🇲🇴', name: 'Macau', aliases: ['Macao'] },
  { code: 'MQ', flag: '🇲🇶', name: 'Martinique' },
  { code: 'NC', flag: '🇳🇨', name: 'New Caledonia' },
  { code: 'MP', flag: '🇲🇵', name: 'Northern Mariana Islands' },
  { code: 'PR', flag: '🇵🇷', name: 'Puerto Rico' },
  { code: 'RE', flag: '🇷🇪', name: 'Reunion', aliases: ['Réunion'] },
  { code: 'SX', flag: '🇸🇽', name: 'Sint Maarten' },
  { code: 'TC', flag: '🇹🇨', name: 'Turks & Caicos Islands', aliases: ['Turks and Caicos Islands'] },
  { code: 'VI', flag: '🇻🇮', name: 'U.S. Virgin Islands', aliases: ['Virgin Islands (US)', 'US Virgin Islands'] },
  { code: 'WF', flag: '🇼🇫', name: 'Wallis & Futuna', aliases: ['Wallis and Futuna'] },

  // Special DXCC entities
  { code: 'AX', flag: '🇦🇶', name: 'Antarctica', aliases: ['Antarctic'] },
  { code: 'SJ', flag: '🇸🇯', name: 'Svalbard', aliases: ['Svalbard & Jan Mayen'] },
];

// Create lookup maps for fast access
const flagByName = new Map<string, string>();
const flagByCode = new Map<string, string>();

// Initialize lookup maps
COUNTRY_FLAGS.forEach(country => {
  const nameLower = country.name.toLowerCase();
  flagByName.set(nameLower, country.flag);
  flagByCode.set(country.code.toLowerCase(), country.flag);

  // Add aliases
  country.aliases?.forEach(alias => {
    flagByName.set(alias.toLowerCase(), country.flag);
  });
});

/**
 * Get flag emoji for a country name
 * @param countryName - The country name (case-insensitive)
 * @returns The flag emoji or undefined if not found
 */
export function getFlagByCountryName(countryName: string | undefined | null): string | undefined {
  if (!countryName) return undefined;
  return flagByName.get(countryName.toLowerCase());
}

/**
 * Get flag emoji for a country code
 * @param countryCode - The ISO 3166-1 alpha-2 code (case-insensitive)
 * @returns The flag emoji or undefined if not found
 */
export function getFlagByCountryCode(countryCode: string | undefined | null): string | undefined {
  if (!countryCode) return undefined;
  return flagByCode.get(countryCode.toLowerCase());
}

/**
 * Component helper: Get flag with fallback
 * Returns the flag emoji or a default placeholder
 */
export function getCountryFlag(countryName: string | undefined | null, fallback: string = ''): string {
  return getFlagByCountryName(countryName) ?? fallback;
}
