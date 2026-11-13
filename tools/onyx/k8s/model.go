package k8s

// The subset of the Kubernetes API onyx renders, in the field order it is written.

type object struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Metadata   metadata `yaml:"metadata"`
	Spec       any      `yaml:"spec,omitempty"`
}

type metadata struct {
	Name        string            `yaml:"name,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

type deploymentSpec struct {
	Selector selector    `yaml:"selector"`
	Template podTemplate `yaml:"template"`
}

type selector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type podTemplate struct {
	Metadata metadata `yaml:"metadata"`
	Spec     podSpec  `yaml:"spec"`
}

type podSpec struct {
	ServiceAccountName string      `yaml:"serviceAccountName"`
	EnableServiceLinks bool        `yaml:"enableServiceLinks"`
	Containers         []container `yaml:"containers"`
}

type container struct {
	Name           string          `yaml:"name"`
	Image          string          `yaml:"image"`
	Ports          []containerPort `yaml:"ports,omitempty"`
	Env            []envVar        `yaml:"env,omitempty"`
	Resources      *resources      `yaml:"resources,omitempty"`
	StartupProbe   *probe          `yaml:"startupProbe,omitempty"`
	LivenessProbe  *probe          `yaml:"livenessProbe,omitempty"`
	ReadinessProbe *probe          `yaml:"readinessProbe,omitempty"`
}

type containerPort struct {
	ContainerPort int32 `yaml:"containerPort"`
}

type resources struct {
	Requests map[string]string `yaml:"requests,omitempty"`
	Limits   map[string]string `yaml:"limits,omitempty"`
}

type envVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type probe struct {
	HTTPGet          httpGet `yaml:"httpGet"`
	PeriodSeconds    int     `yaml:"periodSeconds,omitempty"`
	FailureThreshold int     `yaml:"failureThreshold,omitempty"`
}

type httpGet struct {
	Path string `yaml:"path"`
	Port int32  `yaml:"port"`
}

type serviceSpec struct {
	Selector map[string]string `yaml:"selector"`
	Ports    []servicePort     `yaml:"ports"`
}

type servicePort struct {
	Name string `yaml:"name"`
	Port int32  `yaml:"port"`
}

type ingressSpec struct {
	Rules []ingressRule `yaml:"rules"`
}

type ingressRule struct {
	HTTP httpIngress `yaml:"http"`
}

type httpIngress struct {
	Paths []ingressPath `yaml:"paths"`
}

type ingressPath struct {
	Path     string         `yaml:"path"`
	PathType string         `yaml:"pathType"`
	Backend  ingressBackend `yaml:"backend"`
}

type ingressBackend struct {
	Service ingressService `yaml:"service"`
}

type ingressService struct {
	Name string             `yaml:"name"`
	Port ingressServicePort `yaml:"port"`
}

type ingressServicePort struct {
	Name string `yaml:"name"`
}

type kustomization struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Resources  []string `yaml:"resources"`
}
