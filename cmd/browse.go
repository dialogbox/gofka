// Copyright © 2018 Jason Kim <dialogbox@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"html/template"
	"io"
	"net/http"

	"github.com/labstack/echo"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// browseCmd represents the browse command
var browseCmd = &cobra.Command{
	Use:   "browse",
	Short: "Web interface to browse contents of specific topic of parition",
	Run: func(cmd *cobra.Command, args []string) {
		runBrowserWebServer()
	},
}

func init() {
	rootCmd.AddCommand(browseCmd)

	browseCmd.Flags().StringP("addr", "", ":8080", "Listen address")
	viper.BindPFlag("browser.address", browseCmd.Flags().Lookup("addr"))
}

func runBrowserWebServer() {
	addr := viper.GetString("browser.address")
	e := echo.New()
	e.Renderer = &Template{
		templates: template.Must(template.ParseGlob("public/views/*.html")),
	}

	e.GET("/b", browse)
	e.GET("/b/:topic", browse)
	e.GET("/b/:topic/:partitions", browse)

	e.Logger.Fatal(e.Start(addr))
}

type Template struct {
	templates *template.Template
}

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

func browse(c echo.Context) error {
	return c.Render(http.StatusOK, "browse", "World")
}
