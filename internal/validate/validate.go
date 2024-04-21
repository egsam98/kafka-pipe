package validate

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	entrans "github.com/go-playground/validator/v10/translations/en"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var validate = validator.New(validator.WithRequiredStructEnabled())
var trans ut.Translator

func init() {
	validate = validator.New(validator.WithRequiredStructEnabled())
	validate.RegisterTagNameFunc(func(field reflect.StructField) string {
		if tag := field.Tag.Get("yaml"); tag != "-" {
			return tag
		}
		return ""
	})
	if err := validate.RegisterValidation("default", validateDefault); err != nil {
		panic(err)
	}

	trans, _ = ut.New(en.New()).GetTranslator("en")
	if err := entrans.RegisterDefaultTranslations(validate, trans); err != nil {
		panic(err)
	}
}

func Struct(dst any) error {
	err := validate.Struct(dst)
	var vErrs validator.ValidationErrors
	if !errors.As(err, &vErrs) {
		return err
	}

	errs := make(Errors, len(vErrs))
	for i, fe := range vErrs {
		_, pathPrefix, _ := strings.Cut(fe.Namespace(), ".")
		pathPrefix = strings.ReplaceAll(strings.TrimSuffix(pathPrefix, fe.Field()), ".", ":")
		errs[i] = pathPrefix + fe.Translate(trans)
	}
	return errs
}

func StructFromYAML(dst any, value yaml.Node) error {
	if err := value.Decode(dst); err != nil {
		return errors.Wrapf(err, "decode yaml %q into %T", value.Value, dst)
	}
	return Struct(dst)
}

func validateDefault(fl validator.FieldLevel) bool {
	field := fl.Field()
	if !field.IsZero() {
		return true
	}

	switch field.Interface().(type) {
	case time.Duration:
		dur, err := time.ParseDuration(fl.Param())
		if err != nil {
			panic(err)
		}
		field.Set(reflect.ValueOf(dur))
	default:
		switch kind := field.Kind(); kind {
		case reflect.Bool:
			switch fl.Param() {
			case "true":
				field.SetBool(true)
			case "false":
				field.SetBool(false)
			default:
				panic(errors.Errorf("default: unexpected bool value: %s. Pass true or false", fl.Param()))
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value, err := strconv.ParseInt(fl.Param(), 10, 64)
			if err != nil {
				panic(err)
			}
			field.SetInt(value)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value, err := strconv.ParseUint(fl.Param(), 10, 64)
			if err != nil {
				panic(err)
			}
			field.SetUint(value)
		case reflect.String:
			field.SetString(fl.Param())
		default:
			panic(fmt.Errorf("default: unexpected reflect.Kind: %s", kind))
		}
	}
	return true
}
