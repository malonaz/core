package canonicalize

import (
	"testing"

	"github.com/nyaruka/phonenumbers"
	"github.com/stretchr/testify/require"
)

func TestPhoneNumber(t *testing.T) {
	tests := []struct {
		name    string
		number  string
		region  string
		want    string
		wantErr bool
	}{
		{"already canonicalized UK", "+442079460958", "US", "+442079460958", false},
		{"already canonicalized US", "+12025551234", "US", "+12025551234", false},
		{"raw US number", "2025551234", "US", "+12025551234", false},
		{"US with dashes", "202-555-1234", "US", "+12025551234", false},
		{"US with parens", "(202) 555-1234", "US", "+12025551234", false},
		{"US with spaces", "202 555 1234", "US", "+12025551234", false},
		{"Canadian explicit", "4165551234", "CA", "+14165551234", false},
		{"UK with country code and region", "44 20 7946 0958", "GB", "+442079460958", false},
		{"UK with plus prefix", "+44 20 7946 0958", "US", "+442079460958", false},
		{"empty number", "", "US", "", true},
		{"invalid number", "123", "US", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PhoneNumber(tt.number, tt.region)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPhoneNumberWithRegion(t *testing.T) {
	tests := []struct {
		name    string
		number  string
		want    string
		wantErr bool
	}{
		{"already canonicalized UK", "+442079460958", "+442079460958", false},
		{"already canonicalized US", "+12025551234", "+12025551234", false},
		{"raw US number", "2025551234", "+12025551234", false},
		{"US with dashes", "202-555-1234", "+12025551234", false},
		{"US with parens", "(202) 555-1234", "+12025551234", false},
		{"US with spaces", "202 555 1234", "+12025551234", false},
		{"UK with plus prefix", "+44 20 7946 0958", "+442079460958", false},
		{"empty number", "", "", true},
		{"invalid number", "123", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PhoneNumber(tt.number, RegionCodeUS)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRegionIgnoredWhenNumberHasCountryCode(t *testing.T) {
	tests := []struct {
		name                string
		number              string
		region              string
		expectedCountryCode int32
		expectedResult      string
	}{
		{"UK number (+44) with US region should remain UK", "+442079460958", "US", 44, "+442079460958"},
		{"US number (+1) with UK region should remain US", "+12025551234", "GB", 1, "+12025551234"},
		{"French number (+33) with US region should remain French", "+33123456789", "US", 33, "+33123456789"},
		{"German number (+49) with Japan region should remain German", "+493012345678", "JP", 49, "+493012345678"},
		{"Canadian number (+1) with Australia region should remain Canadian", "+14165551234", "AU", 1, "+14165551234"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedPhoneNumber, err := phonenumbers.Parse(tt.number, tt.region)
			require.NoError(t, err)
			require.Equal(t, tt.expectedCountryCode, parsedPhoneNumber.GetCountryCode())

			result, err := PhoneNumber(tt.number, tt.region)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMixedCanonicalizedNumbersFromDifferentRegions(t *testing.T) {
	tests := []struct {
		name            string
		canonicalized   string
		defaultRegion   string
		expectedResult  string
		expectedCountry string
	}{
		{"UK customer number", "+442079460958", "US", "+442079460958", "GB"},
		{"French customer number", "+33123456789", "US", "+33123456789", "FR"},
		{"German customer number", "+493012345678", "US", "+493012345678", "DE"},
		{"Australian customer number", "+61212345678", "US", "+61212345678", "AU"},
		{"Japan customer number", "+81312345678", "US", "+81312345678", "JP"},
		{"US customer number", "+12025551234", "US", "+12025551234", "US"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := PhoneNumber(tt.canonicalized, tt.defaultRegion)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)

			parsedPhoneNumber, err := phonenumbers.Parse(tt.canonicalized, tt.defaultRegion)
			require.NoError(t, err)

			regionCode := phonenumbers.GetRegionCodeForNumber(parsedPhoneNumber)
			require.Equal(t, tt.expectedCountry, regionCode)
		})
	}
}

func TestRegionUsedWhenNumberLacksCountryCode(t *testing.T) {
	tests := []struct {
		name                string
		number              string
		region              string
		expectedCountryCode int32
		expectedResult      string
	}{
		{"Raw number with US region gets US country code", "2025551234", "US", 1, "+12025551234"},
		{"Same raw number with CA region gets CA country code", "4165551234", "CA", 1, "+14165551234"},
		{"UK number without + requires GB region", "2079460958", "GB", 44, "+442079460958"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedPhoneNumber, err := phonenumbers.Parse(tt.number, tt.region)
			require.NoError(t, err)
			require.Equal(t, tt.expectedCountryCode, parsedPhoneNumber.GetCountryCode())

			result, err := PhoneNumber(tt.number, tt.region)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
