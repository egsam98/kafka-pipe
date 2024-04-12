package validate

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

func init() {
	validate.RegisterTagNameFunc(func(field reflect.StructField) string {
		if tag, ok := field.Tag.Lookup("json"); ok {
			return tag
		}
		return field.Tag.Get("yaml")
	})
	if err := validate.RegisterValidation("default", validateDefault); err != nil {
		panic(err)
	}
}

func Struct(src any) error {
	err := validate.Struct(src)
	var vErrs validator.ValidationErrors
	if !errors.As(err, &vErrs) {
		return err
	}

	errs := make([]string, len(vErrs))
	for i, vErr := range vErrs {
		_, fieldPath, _ := strings.Cut(vErr.Namespace(), ".")
		errs[i] = fmt.Sprintf("failed validation %q for %q parameter", vErr.Tag(), fieldPath)
	}
	return errors.New(strings.Join(errs, "; "))
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
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value, err := strconv.ParseInt(fl.Param(), 10, 64)
			if err != nil {
				panic(err)
			}
			field.SetInt(value)
		case reflect.String:
			field.SetString(fl.Param())
		default:
			panic(fmt.Errorf("unexpected reflect.Kind: %s", kind))
		}
	}
	return true
}
