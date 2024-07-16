// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"reflect"

	"github.com/mitchellh/mapstructure"
)

// StringToPrefixConfigFunc returns a function that converts strings to
// *PrefixConfig value. This is designed to be used with mapstructure.
func StringToPrefixConfigFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(&PrefixConfig{}) {
			return data, nil
		}

		// Convert it by parsing
		p, err := ParsePrefixConfig(data.(string))
		if err != nil {
			return data, err
		}
		return p, nil
	}
}

// MapToPrefixConfigFunc returns a function that converts strings to
// *PrefixConfig value. This is designed to be used with mapstructure.
func MapToPrefixConfigFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.Map {
			return data, nil
		}
		if t != reflect.TypeOf(&PrefixConfig{}) {
			return data, nil
		}

		d, ok := data.(map[string]interface{})
		if !ok {
			return data, nil
		}

		source, ok := d["source_path"].(string)
		if !ok {
			return data, nil
		}

		if dc, ok := d["source_datacenter"].(string); ok {
			source = source + "@" + dc
		}

		dest, ok := d["destination_path"].(string)
		if ok {
			source = source + ":" + dest
		}

		if dc, ok := d["destination_datacenter"].(string); ok {
			source = source + "@" + dc
		}

		// Convert it by parsing
		p, err := ParsePrefixConfig(source)
		if err != nil {
			return data, err
		}
		return p, nil
	}
}

// StringToExcludeConfigFunc returns a function that converts strings to
// *ExcludeConfig value. This is designed to be used with mapstructure.
func StringToExcludeConfigFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(&ExcludeConfig{}) {
			return data, nil
		}

		// Convert it by parsing
		p, err := ParseExcludeConfig(data.(string))
		if err != nil {
			return data, err
		}
		return p, nil
	}
}
