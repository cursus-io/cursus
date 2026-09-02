package wire

import "fmt"

func EncodeNegotiationRequest(request NegotiationRequest) ([]byte, error) {
	if request.MinimumVersion == 0 || request.MaximumVersion < request.MinimumVersion {
		return nil, fmt.Errorf("invalid protocol version range %d..%d", request.MinimumVersion, request.MaximumVersion)
	}
	if len(request.Compressions) == 0 || len(request.Compressions) > int(CompressionLZ4)+1 {
		return nil, fmt.Errorf("invalid compression preference count %d", len(request.Compressions))
	}
	encoder := newBinaryEncoder(64)
	encoder.uint16(request.MinimumVersion)
	encoder.uint16(request.MaximumVersion)
	encoder.uint16(uint16(len(request.Compressions)))
	seen := make(map[Compression]struct{}, len(request.Compressions))
	for _, compression := range request.Compressions {
		if !compression.valid() {
			return nil, fmt.Errorf("unsupported compression %d", compression)
		}
		if _, exists := seen[compression]; exists {
			return nil, fmt.Errorf("duplicate compression %s", compression)
		}
		seen[compression] = struct{}{}
		encoder.append(1)[0] = byte(compression)
	}
	return encoder.result()
}

func DecodeNegotiationRequest(data []byte) (NegotiationRequest, error) {
	decoder := newBinaryDecoder(data)
	request := NegotiationRequest{
		MinimumVersion: decoder.uint16(),
		MaximumVersion: decoder.uint16(),
	}
	count := decoder.uint16()
	if count == 0 || count > uint16(CompressionLZ4)+1 {
		return NegotiationRequest{}, fmt.Errorf("invalid compression preference count %d", count)
	}
	seen := make(map[Compression]struct{}, count)
	request.Compressions = make([]Compression, 0, count)
	for range count {
		field := decoder.take(1)
		if len(field) != 1 {
			return NegotiationRequest{}, decoder.finish()
		}
		compression := Compression(field[0])
		if !compression.valid() {
			return NegotiationRequest{}, fmt.Errorf("unsupported compression %d", compression)
		}
		if _, exists := seen[compression]; exists {
			return NegotiationRequest{}, fmt.Errorf("duplicate compression %s", compression)
		}
		seen[compression] = struct{}{}
		request.Compressions = append(request.Compressions, compression)
	}
	if err := decoder.finish(); err != nil {
		return NegotiationRequest{}, err
	}
	if request.MinimumVersion == 0 || request.MaximumVersion < request.MinimumVersion {
		return NegotiationRequest{}, fmt.Errorf("invalid protocol version range %d..%d", request.MinimumVersion, request.MaximumVersion)
	}
	return request, nil
}

func EncodeNegotiationResponse(response NegotiationResponse) ([]byte, error) {
	if response.Version != ProtocolVersion || !response.Compression.valid() {
		return nil, fmt.Errorf("invalid negotiation response version=%d compression=%d", response.Version, response.Compression)
	}
	encoder := newBinaryEncoder(8)
	encoder.uint16(response.Version)
	field := encoder.append(1)
	if len(field) == 1 {
		field[0] = byte(response.Compression)
	}
	return encoder.result()
}

func DecodeNegotiationResponse(data []byte) (NegotiationResponse, error) {
	decoder := newBinaryDecoder(data)
	response := NegotiationResponse{Version: decoder.uint16()}
	field := decoder.take(1)
	if len(field) == 1 {
		response.Compression = Compression(field[0])
	}
	if err := decoder.finish(); err != nil {
		return NegotiationResponse{}, err
	}
	if response.Version != ProtocolVersion || !response.Compression.valid() {
		return NegotiationResponse{}, fmt.Errorf("invalid negotiation response version=%d compression=%d", response.Version, response.Compression)
	}
	return response, nil
}

func SelectCompression(request NegotiationRequest, supported []Compression) (Compression, error) {
	if request.MinimumVersion > ProtocolVersion || request.MaximumVersion < ProtocolVersion {
		return CompressionNone, fmt.Errorf("wire v2 is outside requested version range %d..%d", request.MinimumVersion, request.MaximumVersion)
	}
	available := make(map[Compression]struct{}, len(supported))
	for _, compression := range supported {
		if !compression.valid() {
			return CompressionNone, fmt.Errorf("server advertises unsupported compression %d", compression)
		}
		available[compression] = struct{}{}
	}
	for _, requested := range request.Compressions {
		if _, ok := available[requested]; ok {
			return requested, nil
		}
	}
	return CompressionNone, fmt.Errorf("no mutually supported compression")
}
