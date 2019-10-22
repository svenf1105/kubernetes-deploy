# frozen_string_literal: true

module Krane
  module TemplateReporting
    def record_invalid_template(err:, filename:, content: nil)
      debug_msg = ColorizedString.new("Invalid template: #{filename}\n").red
      debug_msg += "> Error message:\n#{KubernetesDeploy::FormattedLogger.indent_four(err)}"
      if content
        debug_msg += if content =~ /kind:\s*Secret/
          "\n> Template content: Suppressed because it may contain a Secret"
        else
          "\n> Template content:\n#{KubernetesDeploy::FormattedLogger.indent_four(content)}"
        end
      end
      logger.summary.add_paragraph(debug_msg)
    end

    def record_warnings(warning:, filename:)
      warn_msg = "Template warning: #{filename}\n"
      warn_msg += "> Warning message:\n#{KubernetesDeploy::FormattedLogger.indent_four(warning)}"
      logger.summary.add_paragraph(ColorizedString.new(warn_msg).yellow)
    end
  end
end
