package canonicalize

import (
	"fmt"
	"strings"

	"github.com/nyaruka/phonenumbers"
)

const (
	RegionCodeUS = "US"

	PhoneNumberFormatInternational = phonenumbers.INTERNATIONAL
)

func PhoneNumber(phoneNumber, fallbackRegionCode string) (string, error) {
	parsedPhoneNumber, err := parseAndValidatePhoneNumber(phoneNumber, fallbackRegionCode)
	if err != nil {
		return "", err
	}
	return phonenumbers.Format(parsedPhoneNumber, phonenumbers.E164), nil
}

func FormatPhoneNumber(phoneNumber string, fallbackRegionCode string, format phonenumbers.PhoneNumberFormat) (string, error) {
	parsedPhoneNumber, err := parseAndValidatePhoneNumber(phoneNumber, fallbackRegionCode)
	if err != nil {
		return "", err
	}
	return phonenumbers.Format(parsedPhoneNumber, format), nil
}

func FormatPhoneNumberNational(phoneNumber, fallbackRegionCode string) (string, error) {
	parsedPhoneNumber, err := parseAndValidatePhoneNumber(phoneNumber, fallbackRegionCode)
	if err != nil {
		return "", err
	}
	return phonenumbers.Format(parsedPhoneNumber, phonenumbers.NATIONAL), nil
}

func parseAndValidatePhoneNumber(phoneNumber, fallbackRegionCode string) (*phonenumbers.PhoneNumber, error) {
	if phoneNumber == "" {
		return nil, fmt.Errorf("phone number cannot be empty")
	}
	phoneNumber = strings.Trim(phoneNumber, "\n")

	parsedPhoneNumber, err := phonenumbers.Parse(phoneNumber, fallbackRegionCode)
	if err != nil {
		return nil, fmt.Errorf("failed to parse phone number %q: %w", phoneNumber, err)
	}

	if !phonenumbers.IsValidNumber(parsedPhoneNumber) {
		return nil, fmt.Errorf("phone number %q is not valid", phoneNumber)
	}

	return parsedPhoneNumber, nil
}
