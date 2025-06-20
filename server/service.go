package server

import (
	"context"
	"errors"
	"nrRPC/log"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// Precompute the reflect type for context
var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well
	return isExported(t.Name()) || t.PkgPath() == ""
}

func (s *Server) Register(rcvr interface{}, metadata string) error {
	return s.register(rcvr, "", false)
}

func (s *Server) RegisterName(name string, rcvr interface{}, metadata string) error {
	s.Plugins.DoRegister(name, rcvr, metadata)
	return s.register(rcvr, name, true)
}

func (s *Server) register(rcvr interface{}, name string, useName bool) error {
	s.ServiceMapMu.Lock()
	defer s.ServiceMapMu.Unlock()
	if s.serviceMap == nil {
		s.serviceMap = make(map[string]*service)
	}

	service := new(service)
	service.typ = reflect.TypeOf(rcvr)
	service.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(service.rcvr).Type().Name() // Type
	if useName {
		sname = name
	}
	if sname == "" {
		errorStr := "rpc.Register: no service name for type " + service.typ.String()
		log.Error(errorStr)
		return errors.New(errorStr)
	}
	if !useName && !isExported(sname) {
		errorStr := "rpc.Register: type " + sname + " is not exported"
		log.Error(errorStr)
		return errors.New(errorStr)
	}
	service.name = sname

	// Install the methods
	service.method = suitableMethods(service.typ, true)

	if len(service.method) == 0 {
		errorStr := ""

		// To help the user,see if a pointer receive would work
		method := suitableMethods(reflect.PointerTo(service.typ), false)
		if len(method) != 0 {
			errorStr = "rpc.Register: type " + sname + "has no exported methods of suitable type" +
				" (hint: pass a pointer to value of that type)"
		} else {
			errorStr = "rpc.Register: type " + name + " has no exported methods of suitable type"
		}
		log.Error(errorStr)
		return errors.New(errorStr)
	}
	s.serviceMap[service.name] = service
	return nil
}

// suitableMethods returns suitable Rpc methods of typ,it will report
// error using log if reportErr is true
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported
		if method.PkgPath != "" {
			continue
		}
		// Method needs four ins:receiver,context.Context,*args,*reply
		if mtype.NumIn() != 4 {
			if reportErr {
				log.Info("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}

		// First arg must be context.Context
		ctxType := mtype.In(1)
		if !ctxType.Implements(typeOfContext) {
			if reportErr {
				log.Info("method", mname, "has wrong context type:", ctxType)
			}
			continue
		}

		// Second arg need not be a pointer
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Info(mname, "argument type not exported:", argType)
			}
			continue
		}

		//Third arg must be a pointer
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Info("method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}

		// Reply type must be exported
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Info("method", mname, "reply type not exported:", replyType)
			}
			continue
		}

		// Method needs one out
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Info("method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Info("method", mname, "has wrong returnType:", returnType.String())
			}
			continue
		}
		methods[mname] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
	}
	return methods
}

func (s *service) call(ctx context.Context, mtype *methodType, argv, replyv reflect.Value) error {
	function := mtype.method.Func
	// Invoke the method,providing a new value for the reply
	returnValues := function.Call([]reflect.Value{s.rcvr, reflect.ValueOf(ctx), argv, replyv})
	// The return value for the method is an error
	errInter := returnValues[0].Interface()
	if errInter != nil {
		return errInter.(error)
	}

	return nil
}
