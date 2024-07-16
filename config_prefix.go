// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"strings"

	"github.com/hashicorp/consul-template/config"
	dep "github.com/hashicorp/consul-template/dependency"
)

// PrefixConfig is the representation of a key prefix.
type PrefixConfig struct {
	Dependency            *dep.KVListQuery `mapstructure:"-"`
	DestinationPath       *string          `mapstructure:"destination_path"`
	DestinationDatacenter *string          `mapstructure:"destination_datacenter"`
	SourcePath            *string          `mapstructure:"source_path"`
	SourceDatacenter      *string          `mapstructure:"source_datacenter"`
}

// ParsePrefixConfig parses a prefix of the format "source@dc:destination" into
// the PrefixConfig.
func ParsePrefixConfig(s string) (*PrefixConfig, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("missing prefix")
	}

	parts := strings.SplitN(s, ":", 2)

	var source, destination string
	switch len(parts) {
	case 1:
		source, destination = parts[0], ""
	case 2:
		source, destination = parts[0], parts[1]
	default:
		return nil, fmt.Errorf("invalid format: %q", s)
	}

	if !dep.KVListQueryRe.MatchString(source) {
		return nil, fmt.Errorf("invalid source format: %q", source)
	}
	m := regexpMatch(dep.KVListQueryRe, source)

	srcPrefix, srcDC := m["prefix"], m["dc"]

	if srcPrefix == "" {
		return nil, fmt.Errorf("missing source_prefix")
	}

	if srcDC == "" {
		return nil, fmt.Errorf("missing source_datacenter")
	}

	d, err := dep.NewKVListQuery(source)
	if err != nil {
		return nil, err
	}

	var dstPrefix, dstDC string
	if destination != "" {
		if !dep.KVListQueryRe.MatchString(destination) {
			return nil, fmt.Errorf("invalid destination format: %q", destination)
		}
		m := regexpMatch(dep.KVListQueryRe, destination)

		dstPrefix, dstDC = m["prefix"], m["dc"]

		if dstPrefix == "" {
			return nil, fmt.Errorf("missing destination_prefix")
		}

		if dstDC == "" {
			// Use same datacenter
			dstDC = srcDC
		}
	} else {
		dstPrefix = srcPrefix
		dstDC = srcDC
	}

	return &PrefixConfig{
		Dependency:            d,
		DestinationPath:       config.String(dstPrefix),
		DestinationDatacenter: config.String(dstDC),
		SourcePath:            config.String(srcPrefix),
		SourceDatacenter:      config.String(srcDC),
	}, nil
}

func DefaultPrefixConfig() *PrefixConfig {
	return &PrefixConfig{}
}

func (c *PrefixConfig) Copy() *PrefixConfig {
	if c == nil {
		return nil
	}

	var o PrefixConfig

	o.Dependency = c.Dependency

	o.SourcePath = c.SourcePath
	o.SourceDatacenter = c.SourceDatacenter
	o.DestinationPath = c.DestinationPath
	o.DestinationDatacenter = c.DestinationDatacenter

	return &o
}

func (c *PrefixConfig) Merge(o *PrefixConfig) *PrefixConfig {
	if c == nil {
		if o == nil {
			return nil
		}
		return o.Copy()
	}

	if o == nil {
		return c.Copy()
	}

	r := c.Copy()

	if o.Dependency != nil {
		r.Dependency = o.Dependency
	}

	if o.SourcePath != nil {
		r.SourcePath = o.SourcePath
	}

	if o.SourceDatacenter != nil {
		r.SourceDatacenter = o.SourceDatacenter
	}

	if o.DestinationPath != nil {
		r.DestinationPath = o.DestinationPath
	}

	if o.DestinationDatacenter != nil {
		r.DestinationDatacenter = o.DestinationDatacenter
	}

	return r
}

func (c *PrefixConfig) Finalize() {
	if c.SourcePath == nil {
		c.SourcePath = config.String("")
	}

	if c.SourceDatacenter == nil {
		c.SourceDatacenter = config.String("")
	}

	if c.DestinationPath == nil {
		c.DestinationPath = config.String("")
	}

	if c.DestinationDatacenter == nil {
		c.DestinationDatacenter = config.String("")
	}
}

func (c *PrefixConfig) GoString() string {
	if c == nil {
		return "(*PrefixConfig)(nil)"
	}

	return fmt.Sprintf("&PrefixConfig{"+
		"Dependency:%s, "+
		"SourcePath:%s, "+
		"SourceDatacenter:%s, "+
		"DestinationPath:%s, "+
		"DestinationDatacenter:%s, "+
		"}",
		c.Dependency,
		config.StringGoString(c.SourcePath),
		config.StringGoString(c.SourceDatacenter),
		config.StringGoString(c.DestinationPath),
		config.StringGoString(c.DestinationDatacenter),
	)
}

type PrefixConfigs []*PrefixConfig

func DefaultPrefixConfigs() *PrefixConfigs {
	return &PrefixConfigs{}
}

func (c *PrefixConfigs) Copy() *PrefixConfigs {
	if c == nil {
		return nil
	}

	o := make(PrefixConfigs, len(*c))
	for i, t := range *c {
		o[i] = t.Copy()
	}
	return &o
}

func (c *PrefixConfigs) Merge(o *PrefixConfigs) *PrefixConfigs {
	if c == nil {
		if o == nil {
			return nil
		}
		return o.Copy()
	}

	if o == nil {
		return c.Copy()
	}

	r := c.Copy()

	*r = append(*r, *o...)

	return r
}

func (c *PrefixConfigs) Finalize() {
	if c == nil {
		c = DefaultPrefixConfigs()
	}

	for _, t := range *c {
		t.Finalize()
	}
}

func (c *PrefixConfigs) GoString() string {
	if c == nil {
		return "(*PrefixConfigs)(nil)"
	}

	s := make([]string, len(*c))
	for i, t := range *c {
		s[i] = t.GoString()
	}

	return "{" + strings.Join(s, ", ") + "}"
}
