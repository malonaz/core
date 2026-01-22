module github.com/malonaz/core

go 1.25

replace github.com/bazelbuild/buildtools => github.com/peterebden/buildtools v0.0.0-20201001123124-f7a36c689cc9

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1
	buf.build/go/protovalidate v1.0.0
	github.com/AlexxIT/go2rtc v1.9.12
	github.com/Masterminds/sprig/v3 v3.3.0
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b
	github.com/anthropics/anthropic-sdk-go v1.19.0
	github.com/bazelbuild/buildtools v0.0.0-20250930140053-2eb4fccefb52
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/coder/websocket v1.8.14
	github.com/go-json-experiment/json v0.0.0-20251027170946-4849db3c2f7e
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.7.0
	github.com/google/go-jsonnet v0.21.0
	github.com/google/uuid v1.6.0
	github.com/gordonklaus/portaudio v0.0.0-20250206071425-98a94950218b
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.1.0
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3
	github.com/hashicorp/go-multierror v1.1.1
	github.com/huandu/xstrings v1.5.0
	github.com/improbable-eng/grpc-web v0.15.0
	github.com/jackc/pgerrcode v0.0.0-20250907135507-afb5586c32a6
	github.com/jackc/pgx/v5 v5.7.6
	github.com/jessevdk/go-flags v1.6.1
	github.com/karminski/streaming-json-go v0.0.4
	github.com/lib/pq v1.10.9
	github.com/malonaz/pgq v0.0.0-20251114112516-9240ed92bf50
	github.com/mennanov/fmutils v0.3.3
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/mwitkow/grpc-proxy v0.0.0-20250813121105-2866842de9a5
	github.com/openai/openai-go/v3 v3.10.0
	github.com/please-build/gcfg v1.7.0
	github.com/prometheus/client_golang v1.23.2
	github.com/pseudomuto/protoc-gen-doc v1.5.2-0.20250211140318-129dc82aa469
	github.com/sashabaranov/go-openai v1.41.2
	github.com/scylladb/go-set v1.0.2
	github.com/sercand/kuberesolver/v5 v5.1.1
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cobra v1.10.1
	github.com/spiffe/go-spiffe/v2 v2.5.0
	github.com/stretchr/testify v1.11.1
	github.com/tidwall/pretty v1.2.1
	github.com/xeipuuv/gojsonschema v1.2.0
	go.einride.tech/aip v0.73.0
	golang.org/x/image v0.34.0
	golang.org/x/mod v0.30.0
	golang.org/x/net v0.46.0
	golang.org/x/sync v0.19.0
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822
	google.golang.org/genproto/googleapis/api v0.0.0-20250818200422-3122310a409c
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250818200422-3122310a409c
	google.golang.org/grpc v1.75.1
	google.golang.org/protobuf v1.36.9
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.24.0 // indirect
	dario.cat/mergo v1.0.1 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/semver/v3 v3.3.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/google/cel-go v0.26.1 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/joschi/pgq v0.0.6 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-proto-validators v0.3.2 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtp v1.8.24 // indirect
	github.com/pion/sdp/v3 v3.0.16 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/pseudomuto/protokit v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sigurn/crc16 v0.0.0-20240131213347-83fcde1e29d1 // indirect
	github.com/sigurn/crc8 v0.0.0-20220107193325-2243fe600f9f // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/stoewer/go-strcase v1.3.1 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20180127040702-4e3ac2762d5f // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/exp v0.0.0-20250620022241-b7579e27df2b // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	nhooyr.io/websocket v1.8.6 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
